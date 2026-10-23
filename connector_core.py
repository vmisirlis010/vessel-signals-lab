"""Read-only HTTP GraphQL retrieval. Safe to embed in a Fabric notebook."""

from datetime import datetime, timezone
import json
import math
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID

FIELDS = (
    "reading_id provider vessel_id tag_id tag_name measured_at_utc "
    "value unit quality modified_at_utc revision"
)
PAGE_QUERY = """
query Download($after: String, $filter: measurementsFilterInput) {
  measurements(first: 200, after: $after, filter: $filter,
               orderBy: {reading_id: ASC}) {
    items { READING_FIELDS }
    hasNextPage
    endCursor
  }
}
""".replace("READING_FIELDS", FIELDS)
STATUS_QUERY = """
query Publication {
  publication_statuses(first: 2) {
    items {
      dataset_id provider consumer published_at_utc upper_modified_at_utc
      row_count vessel_count tag_count
    }
    hasNextPage
  }
}
"""
SCHEMA_QUERY = """
query ReadOnlySchema {
  __schema {
    queryType { fields { name } }
    mutationType { fields { name } }
    subscriptionType { fields { name } }
  }
}
"""
ALLOWED_VESSELS = {f"ALPHA-{number:03d}" for number in range(1, 5)}
ALLOWED_TAGS = {f"T{number:02d}" for number in range(1, 16)}


def endpoint_ids(endpoint):
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "https" or not parsed.hostname
        or not re.fullmatch(r"[a-z0-9-]+(?:\.[a-z0-9-]+)*\.graphql\.fabric\.microsoft\.com", parsed.hostname)
        or parsed.port not in (None, 443) or parsed.username or parsed.password
        or parsed.query or parsed.fragment
    ):
        raise ValueError("An uncredentialed HTTPS public Fabric GraphQL endpoint is required.")
    match = re.fullmatch(
        r"/v1/workspaces/([0-9a-fA-F-]{36})/graphqlapis/([0-9a-fA-F-]{36})/graphql/?",
        parsed.path,
    )
    if not match:
        raise ValueError("Copy the Fabric API endpoint, not the browser's item URL.")
    try:
        ids = tuple(str(UUID(value)) for value in match.groups())
    except ValueError:
        raise ValueError("GraphQL endpoint contains invalid resource IDs.") from None
    if any(value == str(UUID(int=0)) for value in ids):
        raise ValueError("GraphQL endpoint contains an empty resource ID.")
    return ids


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("An ISO timestamp string is required.")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("A UTC offset is required on API timestamps.")
    return result.astimezone(timezone.utc)


def validate_reading(row):
    if not isinstance(row, dict) or set(row) != set(FIELDS.split()):
        raise ValueError("Unexpected measurement fields.")
    if row["provider"] != "Fleet Alpha":
        raise ValueError("Unexpected data provider.")
    if row["vessel_id"] not in ALLOWED_VESSELS or row["tag_id"] not in ALLOWED_TAGS:
        raise ValueError("API returned data outside the approved scope.")
    if not isinstance(row["reading_id"], str) or not re.fullmatch(r"[0-9a-f]{32}", row["reading_id"]):
        raise ValueError("Invalid measurement identifier.")
    if type(row["revision"]) is not int or row["revision"] < 1:
        raise ValueError("Invalid measurement revision.")
    if type(row["value"]) not in (int, float) or not math.isfinite(row["value"]):
        raise ValueError("Measurement value must be finite.")
    for field in ("tag_name", "unit", "quality"):
        if not isinstance(row[field], str) or not row[field]:
            raise ValueError("Missing measurement text field.")
    timestamp(row["measured_at_utc"])
    timestamp(row["modified_at_utc"])


class _NoGraphQLRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GraphQLClient:
    def __init__(self, endpoint, access_token, opener=None):
        self.workspace_id, self.api_id = endpoint_ids(endpoint)
        self.endpoint = endpoint
        self.access_token = access_token
        self.opener = opener or build_opener(_NoGraphQLRedirect())

    def query(self, query, variables=None):
        if not (query.lstrip().startswith("{") or re.match(r"^\s*query\b", query)):
            raise ValueError("The consumer accepts read queries only.")
        if re.search(r"\b(mutation|subscription)\b", query):
            raise ValueError("The consumer never submits mutations or subscriptions.")
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        request = Request(self.endpoint, data=body, method="POST", headers={
            "Authorization": "Bearer " + self.access_token(),
            "Content-Type": "application/json",
        })
        try:
            with self.opener.open(request, timeout=90) as response:
                result = json.load(response)
        except HTTPError as error:
            status = error.code
            error.close()
            raise RuntimeError(f"GraphQL HTTP {status}; response suppressed. No POST retry.") from None
        except URLError:
            raise RuntimeError("GraphQL connection failed; no POST retry.") from None
        if not isinstance(result, dict) or result.get("errors") or not isinstance(result.get("data"), dict):
            raise ValueError("GraphQL query failed or returned partial data. No checkpoint was advanced.")
        return result["data"]

    def verify_read_only(self):
        schema = self.query(SCHEMA_QUERY)["__schema"]
        if schema.get("mutationType") and schema["mutationType"].get("fields"):
            raise ValueError("The API exposes mutations. Disable every mutation before attaching.")
        if schema.get("subscriptionType") and schema["subscriptionType"].get("fields"):
            raise ValueError("Unexpected subscription root.")
        roots = {field["name"] for field in schema["queryType"]["fields"]}
        required = {"measurements", "publication_statuses"}
        permitted = required | {"measurements_by_pk", "publication_status_by_pk", "publication_statuses_by_pk"}
        if not required <= roots or roots - permitted:
            raise ValueError("Expose only measurements and publication_status with their default query names.")
        return {"read_only_schema": True, "query_fields": sorted(roots)}

    def publication(self):
        page = self.query(STATUS_QUERY)["publication_statuses"]
        items = page["items"]
        if len(items) != 1 or page.get("hasNextPage"):
            raise ValueError("Expected exactly one published dataset.")
        status = items[0]
        if (status["dataset_id"], status["provider"], status["consumer"]) != (
            "vessel-signals-v1", "Fleet Alpha", "Analytics Beta"
        ):
            raise ValueError("Unexpected synthetic dataset.")
        if status["vessel_count"] != 4 or status["tag_count"] != 15:
            raise ValueError("Unexpected published data scope.")
        count = status["row_count"]
        if type(count) is not int or count < 1440 or (count - 1440) % 60:
            raise ValueError("Publication must contain 1,440 initial rows plus complete 60-row hours.")
        timestamp(status["upper_modified_at_utc"])
        timestamp(status["published_at_utc"])
        return status

    def pages(self, lower, upper):
        cutoff = timestamp(upper)
        floor = timestamp(lower) if lower else None
        if floor and floor > cutoff:
            raise ValueError("Publication watermark moved backwards.")
        filters = {"modified_at_utc": {"lte": upper}}
        if lower is not None:
            filters["modified_at_utc"]["gt"] = lower
        cursor, last_id = None, None
        seen_cursors, seen_ids = set(), set()
        for _ in range(10000):
            page = self.query(PAGE_QUERY, {"after": cursor, "filter": filters})["measurements"]
            rows = page["items"]
            if not isinstance(rows, list) or type(page["hasNextPage"]) is not bool:
                raise ValueError("Malformed GraphQL page.")
            for row in rows:
                validate_reading(row)
                modified = timestamp(row["modified_at_utc"])
                if modified > cutoff or (floor and modified <= floor):
                    raise ValueError("Reading outside the requested publication window.")
                reading_id = row["reading_id"]
                if reading_id in seen_ids or (last_id is not None and reading_id <= last_id):
                    raise ValueError("Duplicate or unordered measurement ID in API pagination.")
                seen_ids.add(reading_id)
                last_id = reading_id
            yield rows
            if not page["hasNextPage"]:
                return
            next_cursor = page.get("endCursor")
            if not rows or not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                raise ValueError("API pagination did not make progress.")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise ValueError("Pagination safety limit reached.")

    def verified_snapshot(self, expected=None):
        """Small lab-only full read, also used to validate the manual GraphQL checkpoint."""
        self.verify_read_only()
        before = self.publication()
        if expected and not self.matches_publication(before, expected):
            raise ValueError("SQL/GraphQL publication has not caught up to this publish job. Retry later.")
        rows = [row for page in self.pages(None, before["upper_modified_at_utc"]) for row in page]
        if len(rows) != before["row_count"]:
            raise ValueError("Visible rows differ from publication status; retry after SQL synchronization.")
        self.unchanged(before)
        return before, rows

    def unchanged(self, before):
        after = self.publication()
        if not self.matches_publication(before, after):
            raise ValueError("Publication changed during retrieval. Retry with no overlapping publisher.")

    @staticmethod
    def matches_publication(actual, expected):
        return (
            actual["row_count"] == expected["row_count"]
            and timestamp(actual["upper_modified_at_utc"]) == timestamp(expected["upper_modified_at_utc"])
            and timestamp(actual["published_at_utc"]) == timestamp(expected["published_at_utc"])
        )

    def wait_for_publication(self, expected, timeout_seconds=600, poll_seconds=15):
        """Poll successful read results for SQL visibility; never retry HTTP or schema failures."""
        deadline = time.monotonic() + timeout_seconds
        while True:
            status = self.publication()
            if self.matches_publication(status, expected):
                return status
            if time.monotonic() + poll_seconds >= deadline:
                raise TimeoutError("SQL/GraphQL publication visibility timed out; no checkpoint advanced.")
            time.sleep(poll_seconds)
