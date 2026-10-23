from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from uuid import NAMESPACE_URL, uuid4, uuid5

ROOT = Path(__file__).resolve().parents[1]


def uid(name):
    return str(uuid5(NAMESPACE_URL, "vessel-signals-lab-offline-tests:" + name))


def config():
    return {"tenant_id": uid("tenant"), "capacity_id": uid("capacity"),
            "workspace_name": "Offline Test Lab", "graphql_api_name": "AnalyticsBetaTelemetryAPI",
            "timeout_seconds": 60, "poll_seconds": 1}


def endpoint(workspace=None, api=None):
    return f"https://unit-test.graphql.fabric.microsoft.com/v1/workspaces/{workspace or uid('workspace')}/graphqlapis/{api or uid('api')}/graphql"


def publication(count=1440, upper="2026-01-02T01:00:00Z", published="2026-01-02T02:00:00Z"):
    return {"dataset_id": "vessel-signals-v1", "provider": "Fleet Alpha", "consumer": "Analytics Beta",
            "published_at_utc": published, "upper_modified_at_utc": upper,
            "row_count": count, "vessel_count": 4, "tag_count": 15}


def row(number=1, revision=1, modified="2026-01-02T01:00:00Z"):
    return {"reading_id": f"{number:032x}", "provider": "Fleet Alpha",
            "vessel_id": f"ALPHA-{1 + (number - 1) % 4:03d}", "tag_id": f"T{1 + (number - 1) % 15:02d}",
            "tag_name": "synthetic", "measured_at_utc": "2026-01-01T00:00:00Z",
            "value": float(number), "unit": "unit", "quality": "GOOD",
            "modified_at_utc": modified, "revision": revision}


@contextmanager
def scratch():
    path = ROOT / "tests" / (".scratch-" + uuid4().hex)
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class FakeGraphQL:
    def __init__(self, count=1440):
        self.rows = [row(number) for number in range(1, count + 1)]
        self.status = publication(count)
        self.calls = []
        self.mutable = False
        self.extra_root = None
        self.after_first_page = None

    def query(self, query, variables=None):
        from connector_core import timestamp
        self.calls.append((query, variables))
        if "__schema" in query:
            roots = ["measurements", "publication_statuses", "measurements_by_pk"]
            if self.extra_root:
                roots.append(self.extra_root)
            return {"__schema": {
                "queryType": {"fields": [{"name": name} for name in roots]},
                "mutationType": {"fields": [{"name": "unsafe"}]} if self.mutable else None,
                "subscriptionType": None,
            }}
        if "publication_statuses" in query:
            return {"publication_statuses": {"items": [deepcopy(self.status)], "hasNextPage": False}}
        filters = variables["filter"]["modified_at_utc"]
        selected = [
            deepcopy(value) for value in self.rows
            if timestamp(value["modified_at_utc"]) <= timestamp(filters["lte"])
            and ("gt" not in filters or timestamp(value["modified_at_utc"]) > timestamp(filters["gt"]))
        ]
        start = int(variables["after"] or 0)
        items = selected[start:start + 200]
        if self.after_first_page and start:
            self.after_first_page()
        return {"measurements": {"items": items, "hasNextPage": start + 200 < len(selected),
                                 "endCursor": str(start + 200)}}

    def client(self):
        from connector_core import GraphQLClient
        client = GraphQLClient(endpoint(), lambda: "not-a-real-token")
        client.query = self.query
        return client


class FakeAuth:
    def token(self, resource=None):
        return "not-a-real-token"


class FakeFabric:
    def __init__(self):
        self.auth = FakeAuth()
        self.calls = []
        self.workspaces = []
        self.items = []
        self.jobs = {}
        self.capacity_state = "Active"
        self.definitions = {}
        self.lakehouse_properties = {}

    def collect(self, path):
        self.calls.append(("GET-list", path))
        if path == "/capacities":
            return [{"id": uid("capacity"), "state": self.capacity_state}]
        if path == "/workspaces":
            return deepcopy(self.workspaces)
        if path.endswith("/jobs/instances"):
            item_id = path.split("/items/")[1].split("/")[0]
            return [deepcopy(job) for job in self.jobs.values() if job["itemId"] == item_id]
        if path.endswith("/items"):
            return deepcopy(self.items)
        raise AssertionError(path)

    def request(self, method, path, body=None):
        from fabric_runtime import FABRIC
        self.calls.append((method, path, deepcopy(body)))
        relative = path.removeprefix(FABRIC)
        if method == "POST" and relative == "/workspaces":
            item = dict(body, id=uid("workspace"), type="Workspace")
            self.workspaces.append(item)
            return 201, {}, deepcopy(item)
        if method == "POST" and relative.endswith(("/lakehouses", "/notebooks", "/items")):
            kind = "Lakehouse" if relative.endswith("/lakehouses") else "Notebook" if relative.endswith("/notebooks") else body["type"]
            item = {"id": uid(body["displayName"]), "displayName": body["displayName"], "type": kind}
            self.items.append(item)
            if kind == "Lakehouse":
                endpoint_id = uid(body["displayName"] + ":sql-endpoint")
                self.items.append({"id": endpoint_id, "displayName": item["displayName"], "type": "SQLEndpoint"})
                self.lakehouse_properties[item["id"]] = {
                    "defaultSchema": "dbo", "sqlEndpointProperties": {"id": endpoint_id, "provisioningStatus": "Success"},
                }
            if body.get("definition"):
                self.definitions[item["id"]] = body["definition"]
            return 201, {}, deepcopy(item)
        if method == "POST" and "/jobs/" in relative and relative.endswith("/instances"):
            item_id = relative.split("/items/")[1].split("/")[0]
            item = next(item for item in self.items if item["id"] == item_id)
            name = item["displayName"]
            output = {"source_rows": 2400} if name.startswith("01_") else publication() if name.startswith("02_") else {"stored_readings": 1440}
            job_id = uid(name + ":job:" + str(len(self.jobs)))
            self.jobs[job_id] = {
                "id": job_id, "itemId": item_id, "status": "Completed",
                "startTimeUtc": datetime.now(timezone.utc).isoformat(), "exitValue": json.dumps(output),
            }
            location = FABRIC + relative.split("/jobs/")[0] + "/jobs/instances/" + job_id
            return 202, {"Location": location, "Retry-After": "0"}, None
        if method == "GET" and "/jobs/instances/" in relative:
            return 200, {}, deepcopy(self.jobs[relative.rsplit("/", 1)[1]])
        if method == "GET" and "/items/" in relative:
            return 200, {}, deepcopy(next(item for item in self.items if item["id"] == relative.rsplit("/", 1)[1]))
        if method == "GET" and "/lakehouses/" in relative:
            lakehouse_id = relative.rsplit("/", 1)[1]
            item = deepcopy(next(item for item in self.items if item["id"] == lakehouse_id))
            item["properties"] = deepcopy(self.lakehouse_properties[lakehouse_id])
            return 200, {}, item
        if method == "GET" and relative.startswith("/workspaces/"):
            return 200, {}, deepcopy(self.workspaces[0])
        if method == "POST" and relative.endswith("/updateDefinition"):
            return 200, {}, None
        raise AssertionError((method, path, body))

    def complete(self, response, expect_result=True):
        if response[0] == 202:
            raise AssertionError("FakeFabric creates are synchronous.")
        return response[2]
