import base64
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
import io
import json
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, parse_qs

import fabric_lab
from fabric_runtime import (
    AzureCLIAuth, FABRIC, FabricClient, FabricHTTPError, FabricOperationFailed, State,
    fabric_url, load_config, retry_after, token_claims,
)
from tests.support import FakeAuth, config, scratch, uid


def jwt(**updates):
    claims = {"tid": uid("tenant"), "exp": 9999999999, "scp": "Fabric.Read.All", "idtyp": "user"}
    claims.update(updates)
    encoded = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return "header." + encoded + ".signature"


class ConfigTests(TestCase):
    def test_config_validation_and_no_unknown_fields(self):
        with scratch() as directory:
            path = directory / "config.json"
            path.write_text(json.dumps(config()), encoding="utf-8")
            self.assertEqual(load_config(path), config())
            for key, value in (("tenant_id", "YOUR-TENANT"), ("capacity_id", uid("empty") + "/x"),
                               ("workspace_name", "../bad"), ("poll_seconds", True), ("extra", "value")):
                invalid = config()
                invalid[key] = value
                path.write_text(json.dumps(invalid), encoding="utf-8")
                with self.subTest(key=key), self.assertRaises(ValueError):
                    load_config(path)

    def test_all_mutating_commands_without_apply_only_plan(self):
        with patch.object(fabric_lab, "AzureCLIAuth", side_effect=AssertionError("no auth")), \
                patch.object(fabric_lab, "State", side_effect=AssertionError("no state")), \
                patch("pathlib.Path.exists", return_value=False), redirect_stdout(io.StringIO()) as output:
            for command in ([], ["plan"], ["prepare"], ["attach"], ["complete"], ["run"],
                            ["reconcile"], ["retry-job"], ["retry-operation"]):
                self.assertEqual(fabric_lab.main(command), 0)
            self.assertIn("no local writes", output.getvalue())

    def test_token_tenant_user_application_and_expiry_guards(self):
        self.assertEqual(token_claims(jwt(), uid("tenant"), user_only=True)["idtyp"], "user")
        for value, options in (
            (jwt(tid=uid("other")), {}), (jwt(exp=0), {}), ("malformed", {}),
            (jwt(idtyp="app", scp=None), {"user_only": True}),
            (jwt(appid=uid("wrong")), {"client_id": uid("app")}),
        ):
            with self.subTest(value=value[:15]), self.assertRaises(ValueError):
                token_claims(value, uid("tenant"), **options)

    def test_azure_cli_failures_never_display_token_or_stderr(self):
        result = Mock(returncode=1, stderr="secret-token", stdout="secret-token")
        with patch("fabric_runtime.shutil.which", return_value="az"), \
                patch("fabric_runtime.subprocess.run", return_value=result):
            with self.assertRaises(RuntimeError) as error:
                AzureCLIAuth(uid("tenant")).token()
            self.assertNotIn("secret-token", str(error.exception))


class TransportTests(TestCase):
    def test_host_and_redirect_guards(self):
        for value in (
            "https://api.fabric.microsoft.com.evil.invalid/v1/operations/x",
            "https://user:password@api.fabric.microsoft.com/v1/operations/x",
            "https://api.fabric.microsoft.com:444/v1/operations/x",
            "http://api.fabric.microsoft.com/v1/operations/x",
            "https://api.fabric.microsoft.com/not-v1/x",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                fabric_url(value)
        self.assertEqual(fabric_url("/capacities"), FABRIC + "/capacities")
        from fabric_runtime import NoRedirect
        self.assertIsNone(NoRedirect().redirect_request(None, None, 302, "", {}, "https://evil.invalid"))

    def test_write_gate_and_no_post_retry_or_error_body(self):
        client = FabricClient(FakeAuth())
        with self.assertRaises(PermissionError):
            client.request("POST", "/workspaces", {})
        opener = Mock()
        opener.open.side_effect = HTTPError(FABRIC, 503, "error", {}, io.BytesIO(b"secret-token"))
        client = FabricClient(FakeAuth(), writes=True, opener=opener)
        with self.assertRaises(FabricHTTPError) as error:
            client.request("POST", "/workspaces", {})
        self.assertEqual(opener.open.call_count, 1)
        self.assertNotIn("secret", str(error.exception))

    def test_lro_honors_retry_after_and_handles_no_result(self):
        client = FabricClient(FakeAuth(), timeout=60)
        responses = [
            (200, {"Retry-After": "7"}, {"status": "Running"}),
            (200, {}, {"status": "Succeeded"}),
        ]
        with patch.object(client, "request", side_effect=responses) as request, \
                patch("fabric_runtime.time.sleep") as sleep:
            self.assertIsNone(client.complete((202, {"Location": FABRIC + "/operations/" + uid("op"),
                                                     "Retry-After": "3"}, None), expect_result=False))
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [3, 7])
            self.assertEqual(request.call_count, 2)

    def test_lro_result_location_and_operation_id_fallback(self):
        client = FabricClient(FakeAuth())
        location = FABRIC + "/operations/" + uid("op")
        with patch.object(client, "request", side_effect=[
            (200, {"Location": location + "/result"}, {"status": "Succeeded"}),
            (200, {}, {"id": uid("created")}),
        ]) as request, patch("fabric_runtime.time.sleep"):
            result = client.complete((202, {"x-ms-operation-id": uid("op"), "Retry-After": "0"}, None))
            self.assertEqual(result["id"], uid("created"))
            self.assertEqual(request.call_args_list[0].args, ("GET", location))
            self.assertEqual(request.call_args_list[1].args, ("GET", location + "/result"))

    def test_lro_timeout_and_foreign_location_fail_closed(self):
        client = FabricClient(FakeAuth(), timeout=1)
        with self.assertRaises(TimeoutError):
            client.complete((202, {"Location": FABRIC + "/operations/" + uid("op"), "Retry-After": "2"}, None))
        with self.assertRaises(ValueError):
            client.complete((202, {"Location": "https://evil.invalid/"}, None))

    def test_lro_terminal_failures_are_typed_without_fetching_result_or_error_body(self):
        for status in ("Failed", "Cancelled", "Canceled"):
            client = FabricClient(FakeAuth())
            response = (202, {"Location": FABRIC + "/operations/" + uid("op"), "Retry-After": "0"}, None)
            with self.subTest(status=status), patch.object(client, "request", return_value=(
                200, {}, {"status": status, "error": {"message": "secret-token"}}
            )) as request, patch("fabric_runtime.time.sleep"):
                with self.assertRaises(FabricOperationFailed) as error:
                    client.complete(response, expect_result=False)
                self.assertEqual(error.exception.status, status)
                self.assertNotIn("secret-token", str(error.exception))
                request.assert_called_once_with("GET", response[1]["Location"])

    def test_pagination_opaque_token_and_cycle_guards(self):
        client = FabricClient(FakeAuth())
        with patch.object(client, "request", side_effect=[
            (200, {}, {"value": [1], "continuationToken": "opaque +&token"}),
            (200, {}, {"value": [2]}),
        ]) as request:
            self.assertEqual(client.collect("/workspaces"), [1, 2])
            query = parse_qs(urlsplit(request.call_args_list[1].args[1]).query)
            self.assertEqual(query["continuationToken"], ["opaque +&token"])
        with patch.object(client, "request", return_value=(200, {}, {
            "value": [], "continuationUri": FABRIC + "/workspaces",
        })):
            with self.assertRaises(ValueError):
                client.collect("/workspaces")
        with patch.object(client, "request", return_value=(200, {}, {
            "value": [], "continuationUri": "https://evil.invalid/",
        })) as request:
            with self.assertRaises(ValueError):
                client.collect("/workspaces")
            self.assertEqual(request.call_count, 1)

    def test_retry_after_date_and_invalid_limits(self):
        self.assertEqual(retry_after({"Retry-After": "60"}), 60)
        future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=60))
        self.assertGreater(retry_after({"Retry-After": future}), 58)
        for value in ("invalid", "-1", "9999999", "nan"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                retry_after({"Retry-After": value})


class StateTests(TestCase):
    def test_state_bound_to_full_normalized_config_and_atomic_save(self):
        with scratch() as directory:
            path = directory / "state.json"
            state = State(config(), path)
            self.assertFalse(path.exists())
            state.save()
            self.assertEqual(State(config(), path).data, state.data)
            self.assertFalse(path.with_suffix(".partial").exists())
            different = config()
            different["workspace_name"] = "Other Workspace"
            with self.assertRaises(ValueError):
                State(different, path)
            with state.lock():
                with self.assertRaises(RuntimeError):
                    with state.lock():
                        pass
            self.assertFalse(path.with_suffix(".lock").exists())

    def test_accepted_operation_resumes_without_post_retry(self):
        with scratch() as directory:
            state = State(config(), directory / "state.json")
            client = Mock()
            response = (202, {"Location": FABRIC + "/operations/" + uid("op")}, None)
            client.request.return_value = response
            client.complete.side_effect = TimeoutError("poll timeout")
            with self.assertRaises(TimeoutError):
                state.write_operation(client, "workspace", "/workspaces", {"displayName": "Test"})
            resumed = State(config(), state.path)
            client.complete.side_effect = None
            client.complete.return_value = {"id": uid("workspace")}
            self.assertEqual(resumed.write_operation(
                client, "workspace", "/workspaces", {"displayName": "Test"}
            )["id"], uid("workspace"))
            self.assertEqual(client.request.call_count, 1)

    def test_unknown_response_and_changed_payload_never_resubmit(self):
        with scratch() as directory:
            state = State(config(), directory / "state.json")
            client = Mock()
            client.request.side_effect = RuntimeError("ambiguous response")
            with self.assertRaises(RuntimeError):
                state.write_operation(client, "workspace", "/workspaces", {})
            with self.assertRaises(RuntimeError):
                state.write_operation(client, "workspace", "/workspaces", {})
            with self.assertRaises(ValueError):
                state.write_operation(client, "workspace", "/workspaces", {"different": True})
            self.assertEqual(client.request.call_count, 1)

    def test_definitive_http_rejection_persists_and_is_not_automatically_retried(self):
        with scratch() as directory:
            state = State(config(), directory / "state.json")
            client = Mock()
            client.request.side_effect = FabricHTTPError(400)
            with self.assertRaises(FabricHTTPError):
                state.write_operation(client, "workspace", "/workspaces", {})
            self.assertEqual(state.data["operations"]["workspace"]["phase"], "rejected")
            with self.assertRaises(RuntimeError):
                state.write_operation(client, "workspace", "/workspaces", {})
            self.assertEqual(client.request.call_count, 1)

    def test_failed_and_cancelled_lros_persist_once_for_create_and_definition_update(self):
        for status in ("Failed", "Cancelled"):
            for expect_result in (True, False):
                with self.subTest(status=status, expect_result=expect_result), scratch() as directory:
                    state = State(config(), directory / "state.json")
                    client = FabricClient(FakeAuth(), writes=True)
                    response = (202, {"Location": FABRIC + "/operations/" + uid("op"), "Retry-After": "0"}, None)
                    key = "workspace" if expect_result else "update:Notebook"
                    path = "/workspaces" if expect_result else "/workspaces/" + uid("workspace") + "/items/" + uid("notebook") + "/updateDefinition"
                    body = {"displayName": config()["workspace_name"]} if expect_result else {"definition": {"parts": []}}
                    with patch.object(client, "request", side_effect=[
                        response, (200, {}, {"status": status}),
                    ]) as request, patch("fabric_runtime.time.sleep"):
                        with self.assertRaises(FabricOperationFailed):
                            state.write_operation(client, key, path, body, expect_result)
                        resumed = State(config(), state.path)
                        pending = resumed.data["operations"][key]
                        self.assertEqual(pending["phase"], "terminal_failed")
                        self.assertEqual(pending["terminal_status"], status)
                        self.assertEqual(pending["polling_location"], response[1]["Location"])
                        with self.assertRaises(FabricOperationFailed):
                            resumed.write_operation(client, key, path, body, expect_result)
                        deployment = fabric_lab.Deployment(config(), resumed, client)
                        with self.assertRaises(FabricOperationFailed):
                            deployment.resume_pending()
                        self.assertEqual([call.args[0] for call in request.call_args_list], ["POST", "GET"])

    def test_terminal_outcome_discovered_during_resume_is_persisted(self):
        with scratch() as directory:
            state = State(config(), directory / "state.json")
            state.data["operations"]["workspace"] = {
                "phase": "accepted", "expect_result": True,
                "response": (202, {"Location": FABRIC + "/operations/" + uid("op"), "Retry-After": "0"}, None),
            }
            state.save()
            client = FabricClient(FakeAuth(), writes=True)
            deployment = fabric_lab.Deployment(config(), state, client)
            with patch.object(client, "request", return_value=(200, {}, {"status": "Cancelled"})) as request, \
                    patch("fabric_runtime.time.sleep"):
                with self.assertRaises(FabricOperationFailed):
                    deployment.resume_pending()
                self.assertEqual(State(config(), state.path).data["operations"]["workspace"]["phase"], "terminal_failed")
                with self.assertRaises(FabricOperationFailed):
                    deployment.resume_pending()
                self.assertEqual(request.call_count, 1)
