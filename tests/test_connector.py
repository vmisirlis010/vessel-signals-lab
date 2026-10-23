from copy import deepcopy
from contextlib import closing
import io
import json
import sqlite3
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from connector_core import GraphQLClient, endpoint_ids, timestamp, validate_reading
from consumer import load_consumer_config, sync
from tests.support import FakeGraphQL, config, endpoint, publication, row, scratch, uid


class ConnectorTests(TestCase):
    def test_endpoint_requires_exact_public_host_and_workspace_api_ids(self):
        self.assertEqual(endpoint_ids(endpoint()), (uid("workspace"), uid("api")))
        for value in (
            endpoint().replace("https:", "http:"),
            endpoint().replace(".fabric.microsoft.com", ".fabric.microsoft.com.evil.invalid"),
            endpoint().replace("unit-test.", "user:secret@unit-test."),
            endpoint() + "?access_token=secret",
            endpoint() + "#secret",
            endpoint().replace("/graphqlapis/", "/items/"),
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                endpoint_ids(value)

    def test_read_only_schema_rejects_mutations_and_extra_sources(self):
        server = FakeGraphQL()
        self.assertTrue(server.client().verify_read_only()["read_only_schema"])
        server.mutable = True
        with self.assertRaises(ValueError):
            server.client().verify_read_only()
        server.mutable = False
        server.extra_root = "private_source"
        with self.assertRaises(ValueError):
            server.client().verify_read_only()

    def test_query_guard_http_failure_is_not_retried_or_disclosed(self):
        opener = Mock()
        client = GraphQLClient(endpoint(), lambda: "secret-token", opener=opener)
        for query in ("mutation { unsafe }", "subscription { unsafe }"):
            with self.assertRaises(ValueError):
                client.query(query)
        opener.open.assert_not_called()
        opener.open.side_effect = HTTPError(endpoint(), 503, "bad", {}, io.BytesIO(b"secret-token"))
        with self.assertRaises(RuntimeError) as error:
            client.query("{ measurements(first:1){ items{reading_id} } }")
        self.assertEqual(opener.open.call_count, 1)
        self.assertNotIn("secret-token", str(error.exception))

    def test_complete_scope_validation_and_finite_values(self):
        validate_reading(row())
        for field, value in (
            ("provider", "Other"), ("vessel_id", "ALPHA-005"), ("tag_id", "T16"),
            ("reading_id", "not-an-id"), ("revision", True), ("revision", 0),
            ("value", float("nan")), ("measured_at_utc", "2026-01-01T00:00:00"),
        ):
            invalid = row()
            invalid[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate_reading(invalid)

    def test_pagination_follows_all_pages_with_strict_incremental_cutoff(self):
        server = FakeGraphQL()
        client = server.client()
        pages = list(client.pages(None, server.status["upper_modified_at_utc"]))
        self.assertEqual(len(pages), 8)
        self.assertEqual(sum(map(len, pages)), 1440)
        self.assertEqual(list(client.pages(server.status["upper_modified_at_utc"],
                                           server.status["upper_modified_at_utc"])), [[]])
        with self.assertRaises(ValueError):
            list(client.pages("2027-01-01T00:00:00Z", server.status["upper_modified_at_utc"]))

    def test_duplicate_ids_and_repeating_cursor_rollback_conditions(self):
        server = FakeGraphQL()
        server.rows[200] = deepcopy(server.rows[199])
        with self.assertRaises(ValueError):
            list(server.client().pages(None, server.status["upper_modified_at_utc"]))
        client = GraphQLClient(endpoint(), lambda: "token")
        client.query = Mock(return_value={"measurements": {
            "items": [row()], "hasNextPage": True, "endCursor": "cursor",
        }})
        with self.assertRaises(ValueError):
            list(client.pages(None, publication()["upper_modified_at_utc"]))

    def test_readings_outside_lower_and_upper_are_rejected(self):
        for modified in ("2025-01-01T00:00:00Z", "2027-01-01T00:00:00Z"):
            client = GraphQLClient(endpoint(), lambda: "token")
            client.query = Mock(return_value={"measurements": {
                "items": [row(modified=modified)], "hasNextPage": False,
            }})
            with self.subTest(modified=modified), self.assertRaises(ValueError):
                list(client.pages("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"))

    def test_snapshot_normalizes_timestamp_offsets_and_rejects_stale_counts(self):
        server = FakeGraphQL()
        expected = publication()
        expected["upper_modified_at_utc"] = "2026-01-02T01:00:00.000000+00:00"
        self.assertEqual(len(server.client().verified_snapshot(expected)[1]), 1440)
        expected["row_count"] = 1500
        with self.assertRaises(ValueError):
            server.client().verified_snapshot(expected)
        server.rows.pop()
        with self.assertRaises(ValueError):
            server.client().verified_snapshot()

    def test_publication_polling_is_bounded_and_never_retries_failed_queries(self):
        client = GraphQLClient(endpoint(), lambda: "token")
        expected = publication()
        stale = publication(1500)
        with patch.object(client, "publication", side_effect=[stale, expected]) as read, \
                patch("connector_core.time.sleep") as sleep:
            self.assertEqual(client.wait_for_publication(expected, 60, 15), expected)
            self.assertEqual(read.call_count, 2)
            sleep.assert_called_once_with(15)
        with patch.object(client, "publication", side_effect=RuntimeError("HTTP 503")) as read:
            with self.assertRaises(RuntimeError):
                client.wait_for_publication(expected)
            self.assertEqual(read.call_count, 1)
        with patch.object(client, "publication", return_value=stale):
            with self.assertRaises(TimeoutError):
                client.wait_for_publication(expected, timeout_seconds=1, poll_seconds=15)


class SQLiteTests(TestCase):
    def local_config(self):
        return {"tenant_id": uid("tenant"), "graphql_endpoint": endpoint(), "authentication": "azure-cli"}

    def stored(self, path):
        with closing(sqlite3.connect(path)) as connection:
            return (
                connection.execute("SELECT reading_id,payload,modified_at,revision FROM readings ORDER BY reading_id").fetchall(),
                connection.execute("SELECT * FROM sync_state").fetchall(),
            )

    def test_initial_repeat_correction_and_insert_are_transactional(self):
        server = FakeGraphQL()
        with scratch() as directory:
            path = directory / "consumer.sqlite"
            first = sync(self.local_config(), path, server.client())
            self.assertEqual((first["inserted"], first["stored_readings"], first["pages"]), (1440, 1440, 8))
            repeat = sync(self.local_config(), path, server.client())
            self.assertEqual((repeat["fetched"], repeat["inserted"], repeat["updated"]), (0, 0, 0))
            changed = "2026-01-03T01:00:00Z"
            server.rows[0].update(value=10.25, revision=2, modified_at_utc=changed, quality="CORRECTED")
            server.rows.extend(row(n, modified=changed) for n in range(1441, 1501))
            server.status = publication(1500, changed, "2026-01-03T02:00:00Z")
            second = sync(self.local_config(), path, server.client())
            self.assertEqual((second["fetched"], second["inserted"], second["updated"]), (61, 60, 1))
            readings, state = self.stored(path)
            self.assertEqual(len(readings), 1500)
            self.assertEqual(readings[0][3], 2)
            self.assertEqual(state[0][3], changed)

    def test_mid_page_error_rolls_back_readings_and_watermark(self):
        server = FakeGraphQL()
        with scratch() as directory:
            path = directory / "consumer.sqlite"
            sync(self.local_config(), path, server.client())
            before = self.stored(path)
            changed = "2026-01-03T01:00:00Z"
            for value in server.rows:
                value.update(revision=2, modified_at_utc=changed, value=value["value"] + 0.25)
            server.rows[250]["vessel_id"] = "ALPHA-005"
            server.status = publication(1440, changed, "2026-01-03T02:00:00Z")
            with self.assertRaises(ValueError):
                sync(self.local_config(), path, server.client())
            self.assertEqual(self.stored(path), before)

    def test_count_freshness_revision_and_endpoint_failures_do_not_advance(self):
        for mode in ("count", "changed-publication", "revision", "endpoint", "tenant"):
            server = FakeGraphQL()
            with scratch() as directory:
                path = directory / "consumer.sqlite"
                sync(self.local_config(), path, server.client())
                before = self.stored(path)
                client = server.client()
                local = self.local_config()
                if mode == "endpoint":
                    local["graphql_endpoint"] = endpoint(api=uid("other-api"))
                elif mode == "tenant":
                    local["tenant_id"] = uid("other-tenant")
                else:
                    changed = "2026-01-03T01:00:00Z"
                    server.rows[0].update(value=12.0, revision=2, modified_at_utc=changed)
                    server.status = publication(1500 if mode == "count" else 1440, changed, "2026-01-03T02:00:00Z")
                    if mode == "revision":
                        server.rows[0]["revision"] = 1
                    if mode == "changed-publication":
                        client.unchanged = Mock(side_effect=ValueError("publication changed"))
                with self.subTest(mode=mode), self.assertRaises(ValueError):
                    sync(local, path, client)
                self.assertEqual(self.stored(path), before)

    def test_environment_auth_requires_explicit_client_id(self):
        with scratch() as directory:
            path = directory / "config.json"
            value = self.local_config()
            value["authentication"] = "environment"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_consumer_config(path)
            value["client_id"] = uid("consumer-app")
            path.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(load_consumer_config(path)["client_id"], uid("consumer-app"))
