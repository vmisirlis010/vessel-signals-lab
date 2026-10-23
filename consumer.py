"""Optional local SQLite sink; data and watermark commit in one transaction."""

import argparse
import json
import os
from pathlib import Path
import sqlite3

from connector_core import GraphQLClient, endpoint_ids, timestamp
from fabric_runtime import AzureCLIAuth, GRAPHQL_RESOURCE, ROOT, guid, token_claims


def load_consumer_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if set(config) - {"tenant_id", "graphql_endpoint", "authentication", "client_id"}:
        raise ValueError("Unknown consumer configuration field.")
    config["tenant_id"] = guid(config.get("tenant_id"))
    endpoint_ids(config.get("graphql_endpoint", ""))
    mode = config.get("authentication", "azure-cli")
    if mode not in ("azure-cli", "environment"):
        raise ValueError("authentication must be azure-cli or environment.")
    if mode == "environment":
        config["client_id"] = guid(config.get("client_id"))
    config["authentication"] = mode
    return config


def make_client(config):
    if config["authentication"] == "azure-cli":
        auth = AzureCLIAuth(config["tenant_id"])
        get_token = lambda: auth.token(GRAPHQL_RESOURCE)
    else:
        def get_token():
            token = os.environ.get("VSL_ACCESS_TOKEN", "")
            claims = token_claims(token, config["tenant_id"], config["client_id"])
            if claims.get("scp") or claims.get("idtyp") not in (None, "app"):
                raise ValueError("Environment authentication requires the dedicated application-only identity.")
            return token
    return GraphQLClient(config["graphql_endpoint"], get_token)


def sync(config, database, client=None):
    client = client or make_client(config)
    client.verify_read_only()
    status = client.publication()
    upper = status["upper_modified_at_utc"]
    database = Path(database)
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database, timeout=30)
    try:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS readings "
            "(reading_id TEXT PRIMARY KEY, payload TEXT NOT NULL, modified_at TEXT NOT NULL, revision INTEGER NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS sync_state "
            "(id INTEGER PRIMARY KEY CHECK(id=1), endpoint TEXT NOT NULL, tenant TEXT NOT NULL, watermark TEXT NOT NULL)"
        )
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")
        previous = connection.execute("SELECT endpoint,tenant,watermark FROM sync_state WHERE id=1").fetchone()
        if previous and (previous[0], previous[1]) != (config["graphql_endpoint"], config["tenant_id"]):
            raise ValueError("The local database belongs to another endpoint or tenant.")
        lower = previous[2] if previous else None
        inserted = updated = fetched = pages = 0
        for rows in client.pages(lower, upper):
            pages += 1
            for row in rows:
                old = connection.execute(
                    "SELECT payload,revision,modified_at FROM readings WHERE reading_id=?", (row["reading_id"],)
                ).fetchone()
                payload = json.dumps(row, sort_keys=True, allow_nan=False)
                modified = timestamp(row["modified_at_utc"]).isoformat(timespec="microseconds")
                if old is not None and (row["revision"] < old[1] or modified < old[2]):
                    raise ValueError("A response would overwrite a newer local reading.")
                if old is not None and row["revision"] == old[1] and payload != old[0]:
                    raise ValueError("A changed reading must increment its revision.")
                connection.execute(
                    "INSERT INTO readings VALUES(?,?,?,?) ON CONFLICT(reading_id) DO UPDATE SET "
                    "payload=excluded.payload,modified_at=excluded.modified_at,revision=excluded.revision",
                    (row["reading_id"], payload, modified, row["revision"]),
                )
                inserted += old is None
                updated += old is not None and old[0] != payload
                fetched += 1
        total = connection.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
        if total != status["row_count"]:
            raise ValueError("Stored count differs from publication. SQL may be catching up; deletes are unsupported.")
        client.unchanged(status)
        connection.execute(
            "INSERT INTO sync_state VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET "
            "endpoint=excluded.endpoint,tenant=excluded.tenant,watermark=excluded.watermark",
            (config["graphql_endpoint"], config["tenant_id"], upper),
        )
        connection.commit()
        return {"fetched": fetched, "inserted": inserted, "updated": updated,
                "pages": pages, "stored_readings": total, "watermark": upper,
                "authentication": config.get("authentication", "test")}
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["check", "sync"])
    parser.add_argument("--config", type=Path, default=ROOT / "consumer-config.json")
    parser.add_argument("--database", type=Path, default=ROOT / "consumer-output" / "analytics_beta.sqlite")
    parser.add_argument("--apply", action="store_true", help="Consent to local SQLite writes (never cloud writes).")
    args = parser.parse_args(argv)
    config = load_consumer_config(args.config)
    if args.operation == "sync" and not args.apply:
        print("Plan: read approved GraphQL data; transactionally upsert local SQLite. Use sync --apply to write.")
        return 0
    client = make_client(config)
    if args.operation == "check":
        status, rows = client.verified_snapshot()
        summary = {"read_only": True, "visible_rows": len(rows), "publication": status,
                   "identity_note": "Azure CLI user auth is not standalone application least-privilege proof."}
    else:
        summary = sync(config, args.database, client)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, OSError, KeyError) as error:
        raise SystemExit(str(error))
