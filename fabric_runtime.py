"""Standard-library Fabric transport, local state, and operator authentication."""

import base64
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID

FABRIC = "https://api.fabric.microsoft.com/v1"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
GRAPHQL_RESOURCE = "https://analysis.windows.net/powerbi/api"
DEFINITE_REJECTIONS = {400, 401, 403, 404, 405, 409, 412, 415, 422, 429}
ROOT = Path(__file__).resolve().parent
STATE_PATH = ROOT / ".vessel-signals" / "state.json"


def guid(value):
    try:
        result = str(UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Replace GUID placeholders with actual directory/resource identifiers.") from None
    if result == str(UUID(int=0)):
        raise ValueError("The zero GUID is not a resource identifier.")
    return result


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_config(path):
    raw = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    allowed = {"tenant_id", "capacity_id", "workspace_name", "graphql_api_name", "timeout_seconds", "poll_seconds"}
    if not isinstance(raw, dict) or set(raw) - allowed:
        raise ValueError("Unknown lab configuration field; use lab-config.example.json.")
    for key in ("tenant_id", "capacity_id"):
        raw[key] = guid(raw.get(key))
    for key in ("workspace_name", "graphql_api_name"):
        value = raw.get(key)
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9 _-]{2,79}", value):
            raise ValueError(f"{key} must be 3-80 ASCII letters, digits, spaces, underscores or hyphens.")
        if value != value.strip():
            raise ValueError(f"{key} must not have leading/trailing spaces.")
    for key, default, low, high in (
        ("timeout_seconds", 1800, 60, 7200), ("poll_seconds", 15, 1, 120)
    ):
        value = raw.setdefault(key, default)
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"{key} must be an integer in [{low}, {high}].")
    return raw


def token_claims(access_token, tenant_id, client_id=None, user_only=False):
    try:
        encoded = access_token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        valid = (
            guid(claims["tid"]) == guid(tenant_id)
            and float(claims["exp"]) > time.time() + 30
        )
        if client_id:
            valid = valid and guid(claims.get("appid") or claims.get("azp")) == guid(client_id)
        if user_only:
            valid = valid and claims.get("idtyp") != "app" and bool(claims.get("scp"))
    except (ValueError, KeyError, IndexError, TypeError):
        valid = False
    if not valid:
        raise ValueError("Token tenant, identity type, application, or expiry did not match. Token not displayed.")
    return claims


class AzureCLIAuth:
    """Azure CLI is the trusted token source; claim checks are not JWT signature verification."""

    def __init__(self, tenant_id):
        self.tenant_id = guid(tenant_id)
        self.cache = {}

    def token(self, resource=FABRIC_RESOURCE):
        cached = self.cache.get(resource)
        if cached and cached[1] > time.time() + 120:
            return cached[0]
        executable = shutil.which("az")
        if not executable:
            raise RuntimeError("Azure CLI required. Sign in explicitly with az login --tenant YOUR-TENANT-GUID.")
        result = subprocess.run(
            [executable, "account", "get-access-token", "--tenant", self.tenant_id,
             "--resource", resource, "--output", "json", "--only-show-errors"],
            capture_output=True, text=True, encoding="utf-8",
        )
        if result.returncode:
            raise RuntimeError("Azure CLI authentication failed. Check your tenant-specific user sign-in.")
        try:
            access_token = json.loads(result.stdout)["accessToken"]
        except (ValueError, KeyError):
            raise RuntimeError("Azure CLI returned no usable token; output suppressed.") from None
        claims = token_claims(access_token, self.tenant_id, user_only=True)
        self.cache[resource] = access_token, claims["exp"]
        return access_token


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def retry_after(headers, fallback=5):
    value = {k.lower(): v for k, v in headers.items()}.get("retry-after")
    if value is None:
        return float(fallback)
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            raise ValueError("Invalid Retry-After header.") from None
    if not 0 <= seconds <= 86400:
        raise ValueError("Retry-After is outside the safety limit.")
    return seconds


def fabric_url(path):
    url = path if path.startswith("https://") else FABRIC + path
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https" or parsed.hostname != "api.fabric.microsoft.com"
        or parsed.port not in (None, 443) or parsed.username or parsed.password
        or parsed.fragment or not parsed.path.startswith("/v1/")
    ):
        raise ValueError("Refusing a URL outside the public Fabric REST API.")
    return url


class FabricHTTPError(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__(f"Fabric HTTP {status}. Service body suppressed; inspect the operation in Fabric.")


class FabricOperationFailed(RuntimeError):
    def __init__(self, status, location, key=None):
        self.status, self.location = status, location
        operation = f"Write '{key}'" if key else "Fabric operation"
        super().__init__(
            f"{operation} ended {status}. No POST was repeated. Inspect status, then explicitly use "
            "retry-operation --resource-key KEY --apply, or reconcile a partially created resource "
            "with --resource-key KEY --resource-id VERIFIED-GUID --apply. "
            "Definition updates can only be retried against their existing tracked item."
        )


class FabricClient:
    def __init__(self, auth, writes=False, timeout=1800, poll=15, opener=None):
        self.auth, self.writes = auth, writes
        self.timeout, self.poll = timeout, poll
        self.opener = opener or build_opener(NoRedirect())

    def request(self, method, path, body=None):
        if method != "GET" and not self.writes:
            raise PermissionError("Cloud writes require --apply after operator consent.")
        url = fabric_url(path)
        data = None if body is None else json.dumps(body).encode()
        deadline = time.monotonic() + self.timeout
        for attempt in range(5):
            request = Request(url, data=data, method=method, headers={
                "Authorization": "Bearer " + self.auth.token(),
                "Content-Type": "application/json",
            })
            try:
                with self.opener.open(request, timeout=min(90, self.timeout)) as response:
                    payload = response.read()
                    return response.status, dict(response.headers), json.loads(payload) if payload else None
            except HTTPError as error:
                if method == "GET" and error.code in (429, 502, 503, 504) and attempt < 4:
                    delay = retry_after(error.headers, 2 ** attempt)
                    error.close()
                    if time.monotonic() + delay >= deadline:
                        raise TimeoutError("Read retry timeout; no write was repeated.") from None
                    time.sleep(delay)
                    continue
                status = error.code
                error.close()
                raise FabricHTTPError(status) from None
            except URLError:
                if method == "GET" and attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError("Fabric connection failed. Writes were not retried.") from None
        raise RuntimeError("Fabric read retry limit reached.")

    def collect(self, path):
        records, visited = [], set()
        for _ in range(10000):
            url = fabric_url(path)
            if url in visited:
                raise ValueError("Fabric pagination repeated a continuation URL.")
            visited.add(url)
            _, _, result = self.request("GET", url)
            if not isinstance(result, dict) or not isinstance(result.get("value"), list):
                raise ValueError("Unexpected Fabric list response.")
            records.extend(result["value"])
            next_path = result.get("continuationUri")
            if not next_path and result.get("continuationToken"):
                parsed = urlsplit(url)
                query = dict(parse_qsl(parsed.query))
                query["continuationToken"] = result["continuationToken"]
                next_path = urlunsplit(parsed._replace(query=urlencode(query)))
            if not next_path:
                return records
            path = next_path
        raise ValueError("Fabric pagination limit reached.")

    def complete(self, response, expect_result=True):
        status, headers, payload = response
        if status != 202:
            return payload
        headers = {k.lower(): v for k, v in headers.items()}
        location = headers.get("location")
        if not location and headers.get("x-ms-operation-id"):
            location = FABRIC + "/operations/" + guid(headers["x-ms-operation-id"])
        if not location:
            raise RuntimeError("Accepted operation has no polling location; do not repeat the write.")
        location = fabric_url(location)
        deadline = time.monotonic() + self.timeout
        while True:
            delay = retry_after(headers, self.poll)
            if time.monotonic() + delay >= deadline:
                raise TimeoutError("Operation timeout. Resume the same command to poll, not resubmit.")
            time.sleep(delay)
            _, polled_headers, operation = self.request("GET", location)
            state = (operation or {}).get("status")
            headers = {k.lower(): v for k, v in polled_headers.items()}
            if state == "Succeeded":
                if not expect_result:
                    return None
                result_url = headers.get("location") or location.rstrip("/") + "/result"
                return self.request("GET", fabric_url(result_url))[2]
            if state in ("Failed", "Cancelled", "Canceled"):
                raise FabricOperationFailed(state, location)
            if state not in ("NotStarted", "Running", "InProgress", "Undefined"):
                raise ValueError("Unexpected long-running operation status.")


class State:
    def __init__(self, config, path=STATE_PATH):
        self.path = Path(path)
        self.config_hash = digest(config)
        if self.path.exists():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
            if self.data.get("version") != 1 or self.data.get("config_hash") != self.config_hash:
                raise ValueError("State belongs to different configuration. Do not delete it to reuse existing resources.")
        else:
            self.data = {"version": 1, "config_hash": self.config_hash,
                         "tenant_id": config["tenant_id"], "items": {}, "operations": {}, "jobs": {}}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        partial = self.path.with_suffix(".partial")
        with partial.open("w", encoding="utf-8") as output:
            json.dump(self.data, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        # Antivirus/OneDrive can briefly hold the destination open on Windows.
        for attempt in range(10):
            try:
                partial.replace(self.path)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05 * (attempt + 1))

    @contextmanager
    def lock(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        path = self.path.with_suffix(".lock")
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise RuntimeError("Deployment is locked. Verify no other process is running before removing state.lock.") from None
        try:
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            # Another deployment may have finished between construction and lock acquisition.
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if data.get("config_hash") != self.config_hash:
                    raise ValueError("Configuration changed while acquiring deployment lock.")
                self.data = data
            yield self
        finally:
            path.unlink()

    def write_operation(self, client, key, path, body, expect_result=True):
        operations = self.data["operations"]
        fingerprint = digest({"path": path, "body": body})
        entry = operations.get(key)
        if entry and entry["fingerprint"] != fingerprint:
            raise ValueError("Pending operation payload changed; refusing to replace its intent.")
        if not entry:
            entry = {"fingerprint": fingerprint, "path": path, "phase": "submitting",
                     "expect_result": expect_result, "requested_name": body.get("displayName"),
                     "operation_kind": "definition_update" if key.startswith("update:") else "create",
                     "definition_hash": digest(body["definition"]) if body.get("definition") else None}
            operations[key] = entry
            self.save()
            try:
                response = client.request("POST", path, body)
            except FabricHTTPError as error:
                if error.status in DEFINITE_REJECTIONS:
                    entry.update(phase="rejected", http_status=error.status)
                    self.save()
                raise
            entry["response"] = response
            entry["phase"] = "accepted"
            self.save()
        return self.complete_operation(client, key)

    def complete_operation(self, client, key):
        """Record terminal LRO outcomes before they can strand a resumable intent."""
        entry = self.data["operations"][key]
        if entry["phase"] == "submitting":
            raise RuntimeError(
                f"Ambiguous write '{key}'. Inspect Fabric, then use reconcile with the verified resource ID. "
                "The POST will not be repeated."
            )
        if entry["phase"] == "rejected":
            raise RuntimeError(f"Write '{key}' was rejected. Correct the cause, then explicitly retry-operation.")
        if entry["phase"] == "terminal_failed":
            raise FabricOperationFailed(entry["terminal_status"], entry["polling_location"], key)
        if entry["phase"] != "accepted":
            raise ValueError("Unknown saved operation phase; refusing to repeat the write.")
        try:
            return client.complete(entry["response"], expect_result=entry["expect_result"])
        except FabricOperationFailed as error:
            entry.update(phase="terminal_failed", terminal_status=error.status, polling_location=error.location)
            self.save()
            raise FabricOperationFailed(error.status, error.location, key) from None
