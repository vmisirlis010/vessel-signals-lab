"""Two-phase deployment of an isolated, entirely synthetic Fabric demonstration."""

import argparse
from datetime import datetime, timezone
import json
import re
import sys
import time

from connector_core import GraphQLClient, endpoint_ids, timestamp
from fabric_runtime import (
    AzureCLIAuth, DEFINITE_REJECTIONS, FABRIC, GRAPHQL_RESOURCE, ROOT, FabricClient, FabricHTTPError, State,
    digest, fabric_url, guid, load_config, retry_after,
)
from lab_notebooks import (
    CONSUME, LAKEHOUSES, NOTEBOOKS, PIPELINE, PUBLISH, SEED, SIMULATE,
    documents, notebook_definition, pipeline_content, pipeline_definition, write_generated,
)

DESCRIPTION = "Synthetic Vessel Signals Lab: fictitious Fleet Alpha to Analytics Beta; no real customer data."
ACTIVE = {"NotStarted", "InProgress", "Running"}
TERMINAL_BAD = {"Failed", "Cancelled", "Canceled", "Deduped", "Rejected"}


def job_timestamp(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def plan(config=None, stage="plan"):
    return {
        "mode": "PLAN ONLY: no cloud calls, no tokens, no local writes",
        "requested_stage": stage,
        "workspace": config["workspace_name"] if config else "Set workspace_name in lab-config.json",
        "capacity": "Use the explicitly selected existing ACTIVE capacity; never create/resume/resize capacity.",
        "phase_1": [
            "Preflight user-token tenant, existing capacity and workspace/item naming collisions.",
            "Create a NEW dedicated workspace and three schema-enabled lakehouses.",
            "Generate and deploy seed, approved publisher and simulation notebooks.",
            "Run seed and publisher once, persisting operation/job IDs before polling.",
        ],
        "manual_checkpoint": [
            "In Fabric create the configured API for GraphQL in this dedicated workspace.",
            "Choose SSO and ONLY AnalyticsBetaExport's SQL analytics endpoint.",
            "Expose ONLY dbo.measurements (key reading_id) and dbo.publication_status (key dataset_id).",
            "Keep the default query names measurements and publication_statuses.",
            "Disable every mutation; a lakehouse SQL endpoint is already query-only.",
            "Copy endpoint, then attach --endpoint URL --confirm-read-only --apply.",
        ],
        "phase_2": [
            "Attach verifies endpoint workspace/API, schema, exact publication, full count and scope.",
            "complete --apply deploys/runs HTTP consumer and creates an ON-DEMAND pipeline; no schedule.",
            "SQL visibility is asynchronous. A stale publication FAILS; a fixed sleep is not correctness.",
        ],
        "limits": [
            "No existing-project adoption, capacity operations, IAM changes, app registration or scheduling.",
            "Notebook execution identity is NOT standalone app-only least-privilege proof.",
            "No deletes, no snapshot-isolation guarantee and no transaction across Delta tables.",
            "Repeat a timed-out command with the SAME run ID to poll; never blindly repeat a POST.",
        ],
    }


def manual_steps(config, state):
    workspace = state.data["workspace"]["id"]
    export_id = state.data["items"]["AnalyticsBetaExport"]["id"]
    return {
        "stage": "MANUAL GRAPHQL CHECKPOINT; prepare is complete, deployment is not complete",
        "workspace_url": f"https://app.fabric.microsoft.com/groups/{workspace}/list",
        "steps": [
            f"Create a NEW API for GraphQL named {config['graphql_api_name']!r} in this workspace.",
            f"Select SSO. Select ONLY AnalyticsBetaExport SQL analytics endpoint (lakehouse ID {export_id}).",
            "Wait for SQL table discovery. Expose ONLY dbo.measurements and dbo.publication_status.",
            "If asked for keys, choose reading_id for measurements and dataset_id for publication_status.",
            "Keep default query names measurements and publication_statuses. Do not add other data sources.",
            "In Schema explorer disable ALL mutations, if any. Lakehouse SQL endpoints only support reads.",
            "Use Copy endpoint on the API toolbar (not the browser URL).",
            "Run: python fabric_lab.py attach --config lab-config.json --endpoint \"COPIED-ENDPOINT\" --confirm-read-only --apply",
            "Then: python fabric_lab.py complete --config lab-config.json --apply",
        ],
        "sql_delay": "No fixed delay guarantees SQL readiness. If attach detects stale/incomplete data, wait and rerun attach.",
        "permissions": "No standalone app is created or granted a workspace role by this deployment.",
        "references": [
            "https://learn.microsoft.com/fabric/data-engineering/get-started-api-graphql",
            "https://learn.microsoft.com/fabric/data-engineering/graphql-schema-view",
        ],
    }


class Deployment:
    def __init__(self, config, state, client):
        self.config, self.state, self.client = config, state, client

    def preflight(self, permit_manual_api=False, workspace_override=None, item_overrides=None):
        capacities = self.client.collect("/capacities")
        selected = [item for item in capacities if guid(item["id"]) == self.config["capacity_id"]]
        if len(selected) != 1 or selected[0].get("state") != "Active":
            raise ValueError("Configured capacity must already be accessible and Active. No capacity changes are attempted.")
        matches = [
            item for item in self.client.collect("/workspaces")
            if item["displayName"].casefold() == self.config["workspace_name"].casefold()
        ]
        saved_workspace = self.state.data.get("workspace")
        if workspace_override and saved_workspace and workspace_override["id"] != saved_workspace["id"]:
            raise ValueError("Cannot reconcile a different workspace over the tracked workspace.")
        tracked = workspace_override or saved_workspace
        if len(matches) > 1:
            raise ValueError("Ambiguous workspace name.")
        if matches and (not tracked or matches[0]["id"] != tracked["id"]):
            raise ValueError(
                "An untracked workspace uses this name. If a saved creation produced it, reconcile its verified ID; "
                "otherwise choose a NEW workspace_name. Never adopt unrelated workspaces."
            )
        if tracked:
            if not matches:
                raise ValueError("Tracked workspace is missing or renamed; refusing to recreate it.")
            workspace = self.client.request("GET", "/workspaces/" + guid(tracked["id"]))[2]
            if guid(workspace.get("capacityId")) != self.config["capacity_id"]:
                raise ValueError("Workspace is not assigned to the configured active capacity.")
            self.check_items(permit_manual_api=permit_manual_api, item_overrides=item_overrides,
                             workspace_id=tracked["id"])
        return {"tenant_verified_by_auth": True, "capacity": "Active",
                "workspace": "tracked" if tracked else "new name available", "cloud_writes": False}

    def resume_pending(self):
        """Finish already-accepted writes using GETs before collision checks."""
        for key, pending in list(self.state.data["operations"].items()):
            result = self.state.complete_operation(self.client, key)
            if key == "workspace":
                self.validate_item(result, self.config["workspace_name"], "Workspace")
                self.state.data["workspace"] = result
            elif key.startswith("create:"):
                name = key[len("create:"):]
                kind = "Lakehouse" if name in LAKEHOUSES else "Notebook" if name in NOTEBOOKS else "DataPipeline"
                self.validate_item(result, name, kind)
                result["type"] = kind
                if pending.get("definition_hash"):
                    result["definition_hash"] = pending["definition_hash"]
                self.state.data["items"][name] = result
            elif key.startswith("update:"):
                name = key[len("update:"):]
                self.state.data["items"][name]["definition_hash"] = pending["definition_hash"]
            else:
                raise ValueError("Unknown pending operation.")
            self.state.data["operations"].pop(key)
            self.state.save()

    def prefix(self):
        return "/workspaces/" + guid(self.state.data["workspace"]["id"])

    def check_items(self, permit_manual_api=False, item_overrides=None, workspace_id=None):
        prefix = "/workspaces/" + guid(workspace_id or self.state.data["workspace"]["id"])
        items = self.client.collect(prefix + "/items")
        tracked_items = {**self.state.data["items"], **(item_overrides or {})}
        names = {name: "Lakehouse" for name in LAKEHOUSES}
        names.update({name: "Notebook" for name in NOTEBOOKS})
        names[PIPELINE] = "DataPipeline"
        names[self.config["graphql_api_name"]] = "GraphQLApi"
        for name, kind in names.items():
            matches = [item for item in items if item["displayName"].casefold() == name.casefold()]
            tracked = tracked_items.get(name)
            if kind == "Lakehouse" and any(item["type"] == "SQLEndpoint" for item in matches):
                lakehouses = [item for item in matches if item["type"] == "Lakehouse"]
                endpoints = [item for item in matches if item["type"] == "SQLEndpoint"]
                if len(lakehouses) != 1 or not tracked or lakehouses[0]["id"] != tracked["id"]:
                    raise ValueError(f"Untracked or ambiguous lakehouse/SQL endpoint collision: {name}.")
                lakehouse = self.client.request("GET", prefix + "/lakehouses/" + guid(tracked["id"]))[2]
                self.validate_item(lakehouse, name, "Lakehouse")
                if lakehouse["id"] != tracked["id"]:
                    raise ValueError(f"Lakehouse lookup returned another resource: {name}.")
                properties = lakehouse.get("properties") or {}
                sql_endpoint = properties.get("sqlEndpointProperties") or {}
                endpoint_id = sql_endpoint.get("id")
                if not endpoint_id:
                    raise ValueError(
                        f"Cannot yet verify {name}'s SQL endpoint association. Wait for provisioning and rerun preflight."
                    )
                if len(endpoints) != 1 or guid(endpoints[0]["id"]) != guid(endpoint_id):
                    raise ValueError(f"Unrelated or duplicate SQL endpoint uses the lakehouse name: {name}.")
                # Only the endpoint identified by this tracked lakehouse is an allowed same-name companion.
                matches = [item for item in matches if item["type"] != "SQLEndpoint"]
            if len(matches) > 1:
                raise ValueError(f"Ambiguous item name: {name}.")
            if name == self.config["graphql_api_name"]:
                tracked = self.state.data.get("graphql")
                if matches and not tracked and permit_manual_api and matches[0]["type"] == kind:
                    continue
            if matches and (not tracked or matches[0]["id"] != tracked["id"] or matches[0]["type"] != kind):
                raise ValueError(
                    f"Untracked item/type collision: {name}. If a saved creation produced it, reconcile its verified ID; "
                    "never adopt an unrelated resource."
                )
            if tracked and not matches:
                raise ValueError(f"Tracked item is missing or renamed: {name}.")
        return items

    def ensure_workspace(self):
        if self.state.data.get("workspace"):
            return
        key = "workspace"
        body = {"displayName": self.config["workspace_name"], "description": DESCRIPTION,
                "capacityId": self.config["capacity_id"]}
        item = self.state.write_operation(self.client, key, "/workspaces", body)
        self.validate_item(item, self.config["workspace_name"], "Workspace")
        self.state.data["workspace"] = item
        self.state.data["operations"].pop(key)
        self.state.save()

    @staticmethod
    def validate_item(item, name, kind):
        if not isinstance(item, dict) or item.get("displayName") != name or item.get("type", kind) != kind:
            raise ValueError("Created resource result did not match its requested name/type.")
        guid(item.get("id"))

    def ensure_item(self, name, kind, definition=None):
        tracked = self.state.data["items"].get(name)
        content_hash = digest(definition) if definition else None
        if tracked:
            if definition and tracked.get("definition_hash") != content_hash:
                self.require_idle()
                key = "update:" + name
                path = self.prefix() + "/items/" + tracked["id"] + "/updateDefinition"
                self.state.write_operation(self.client, key, path, {"definition": definition}, expect_result=False)
                tracked["definition_hash"] = content_hash
                self.state.data["operations"].pop(key)
                self.state.save()
            return tracked
        self.check_items(permit_manual_api=self.state.data.get("stage") == "waiting-for-graphql")
        key = "create:" + name
        body = {"displayName": name, "description": DESCRIPTION}
        if kind == "Lakehouse":
            path = self.prefix() + "/lakehouses"
            body["creationPayload"] = {"enableSchemas": True}
        elif kind == "Notebook":
            path = self.prefix() + "/notebooks"
            body["definition"] = definition
        else:
            path = self.prefix() + "/items"
            body.update(type=kind, definition=definition)
        item = self.state.write_operation(self.client, key, path, body)
        self.validate_item(item, name, kind)
        item["type"] = kind
        if definition:
            item["definition_hash"] = content_hash
        self.state.data["items"][name] = item
        self.state.data["operations"].pop(key)
        self.state.save()
        return item

    def require_idle(self, ignore_run_key=None):
        for key, job in self.state.data["jobs"].items():
            if key != ignore_run_key and job.get("status") in ACTIVE | {"submitting"}:
                raise ValueError("Another tracked job is active/ambiguous; resume it before submitting other work.")
        # Pipelines may start notebooks outside this CLI's own tracked job keys.
        for item in self.state.data["items"].values():
            if item.get("type") not in ("Notebook", "DataPipeline"):
                continue
            path = self.prefix() + "/items/" + item["id"] + "/jobs/instances"
            for job in self.client.collect(path):
                if job.get("status") in ACTIVE:
                    raise ValueError("A Fabric job is already active. Monitor it instead of creating a duplicate.")

    def run(self, name, run_key, parameters=None):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,100}", run_key):
            raise ValueError("run-id must be a short alphanumeric identifier (._:- are also allowed).")
        item = self.state.data["items"][name]
        if name == SIMULATE and self.state.data.get("stage") != "ready":
            raise ValueError("Complete initial deployment before intentionally simulating changes.")
        jobs = self.state.data["jobs"]
        job = jobs.get(run_key)
        fingerprint = digest({"item_id": item["id"], "parameters": parameters or {}})
        if job and job.get("fingerprint") != fingerprint:
            raise ValueError("run-id is bound to another item or parameter set; choose a new ID for intentional work.")
        if not job:
            self.require_idle()
            history = self.client.collect(self.prefix() + "/items/" + item["id"] + "/jobs/instances")
            now = datetime.now(timezone.utc)
            recent = []
            for previous in history:
                started = previous.get("startTimeUtc")
                if started:
                    delta = (now - job_timestamp(started)).total_seconds()
                    if delta < 300:
                        recent.append(previous)
            if recent:
                raise ValueError("This item ran within five minutes outside this run-id. Wait; do not create an accidental duplicate.")
            kind = "Pipeline" if item["type"] == "DataPipeline" else "RunNotebook"
            path = self.prefix() + "/items/" + item["id"] + "/jobs/" + kind + "/instances"
            job = {"item_name": name, "item_id": item["id"], "fingerprint": fingerprint,
                   "status": "submitting", "submitted_at": now.isoformat(), "job_type": kind,
                   "parameters": parameters or {}}
            jobs[run_key] = job
            self.state.save()
            body = {"executionData": {"parameters": parameters}} if parameters else {}
            try:
                status, headers, _ = self.client.request("POST", path, body)
            except FabricHTTPError as error:
                if error.status in DEFINITE_REJECTIONS:
                    job.update(status="Rejected", http_status=error.status)
                    self.state.save()
                raise
            if status != 202:
                raise RuntimeError("Expected an accepted job response. Do not retry the submission.")
            headers = {key.lower(): value for key, value in headers.items()}
            location = fabric_url(headers.get("location", ""))
            match = re.fullmatch(
                re.escape(FABRIC + self.prefix() + "/items/" + item["id"] + "/jobs/instances/") + r"([0-9a-fA-F-]{36})",
                location,
            )
            if not match:
                raise ValueError("Unexpected job polling location. Submission will not be repeated.")
            job.update(location=location, id=guid(match[1]), status="NotStarted",
                       retry_after=retry_after(headers, self.config["poll_seconds"]))
            self.state.save()
        if job["status"] == "submitting":
            raise RuntimeError("Ambiguous job submission. Use reconcile --job-key RUN-ID --resource-id VERIFIED-JOB-ID --apply.")
        if job["status"] == "Completed":
            return job["result"]
        if job["status"] in TERMINAL_BAD:
            raise RuntimeError("This run failed/cancelled. Inspect Fabric; a new intentional attempt requires a NEW run-id.")
        deadline = time.monotonic() + self.config["timeout_seconds"]
        delay = job.get("retry_after", self.config["poll_seconds"])
        while True:
            if time.monotonic() + delay >= deadline:
                raise TimeoutError("Job timeout. Resume with the SAME run-id; no duplicate will be submitted.")
            time.sleep(delay)
            _, headers, result = self.client.request("GET", job["location"])
            status = result.get("status")
            job["status"] = status
            job["result"] = result
            self.state.save()
            if status == "Completed":
                return result
            if status in TERMINAL_BAD:
                raise RuntimeError("Fabric job " + status + ". Inspect Fabric before requesting a new intentional attempt.")
            if status not in ACTIVE:
                raise ValueError("Unexpected Fabric job status.")
            delay = retry_after(headers, self.config["poll_seconds"])

    def exit_value(self, name, run_key, result):
        value = result.get("exitValue")
        if value is None:
            job = self.state.data["jobs"][run_key]
            # Documented notebook-public-api endpoint includes notebook exit values.
            path = self.prefix() + "/notebooks/" + job["item_id"] + "/jobs/execute/instances/" + job["id"] + "?beta=true"
            value = self.client.request("GET", path)[2].get("exitValue")
        if not isinstance(value, str):
            raise ValueError(f"{name} completed without an exit value. Inspect notebook outputs; do not rerun the job blindly.")
        try:
            return json.loads(value)
        except ValueError:
            raise ValueError("Notebook exit value was not the expected synthetic JSON result.") from None

    def prepare(self):
        self.preflight(permit_manual_api=self.state.data.get("stage") == "waiting-for-graphql")
        self.ensure_workspace()
        for name in LAKEHOUSES:
            self.ensure_item(name, "Lakehouse")
        for name, document in documents(self.state.data).items():
            write_generated(name + ".ipynb", document)
            self.ensure_item(name, "Notebook", notebook_definition(document))
        result = self.run(SEED, "initial:seed")
        seeded = self.exit_value(SEED, "initial:seed", result)
        if seeded.get("source_rows") != 2400:
            raise ValueError("Initial seed must contain exactly 2,400 source readings.")
        result = self.run(PUBLISH, "initial:publish")
        published = self.exit_value(PUBLISH, "initial:publish", result)
        if published.get("row_count") != 1440:
            raise ValueError("Initial publication must contain exactly 1,440 readings.")
        self.state.data["publication"] = published
        if self.state.data.get("stage") not in ("attached", "ready"):
            self.state.data["stage"] = "waiting-for-graphql"
        self.state.save()
        return manual_steps(self.config, self.state)

    def graphql_client(self, endpoint):
        return GraphQLClient(endpoint, lambda: self.client.auth.token(GRAPHQL_RESOURCE))

    def attach(self, endpoint, confirmed):
        if not confirmed:
            raise ValueError("Review the manual source/table/mutation settings, then use --confirm-read-only.")
        if self.state.data.get("stage") not in ("waiting-for-graphql", "attached", "ready"):
            raise ValueError("Run prepare --apply successfully before attaching an API.")
        self.preflight(permit_manual_api=True)
        workspace_id, api_id = endpoint_ids(endpoint)
        if workspace_id != self.state.data["workspace"]["id"]:
            raise ValueError("GraphQL endpoint belongs to another workspace.")
        existing = self.state.data.get("graphql")
        if existing and (existing["id"] != api_id or existing["endpoint"] != endpoint):
            raise ValueError("Deployment is already bound to another GraphQL endpoint.")
        item = self.client.request("GET", self.prefix() + "/items/" + api_id)[2]
        self.validate_item(item, self.config["graphql_api_name"], "GraphQLApi")
        client = self.graphql_client(endpoint)
        status, rows = client.verified_snapshot(expected=self.state.data["publication"])
        self.state.data["graphql"] = {"id": api_id, "endpoint": endpoint, "confirmed_export_only": True}
        if self.state.data.get("stage") != "ready":
            self.state.data["stage"] = "attached"
        self.state.save()
        return {"stage": self.state.data["stage"], "verified_rows": len(rows), "read_only_schema": True,
                "publication": status, "next": "python fabric_lab.py complete --config lab-config.json --apply",
                "limit": "Schema validation does not prove data-source lineage or standalone application least privilege."}

    def complete(self):
        if self.state.data.get("stage") not in ("attached", "ready"):
            raise ValueError("Complete the manual GraphQL checkpoint and attach first.")
        self.preflight()
        self.graphql_client(self.state.data["graphql"]["endpoint"]).verified_snapshot(
            expected=self.state.data["publication"]
        )
        document = documents(self.state.data, include_consumer=True)[CONSUME]
        write_generated(CONSUME + ".ipynb", document)
        self.ensure_item(CONSUME, "Notebook", notebook_definition(document))
        result = self.run(CONSUME, "initial:consume")
        summary = self.exit_value(CONSUME, "initial:consume", result)
        if summary.get("stored_readings") != 1440:
            raise ValueError("Initial cloud landing must contain exactly 1,440 readings.")
        content = pipeline_content(self.state.data)
        write_generated("pipeline-content.json", content)
        self.ensure_item(PIPELINE, "DataPipeline", pipeline_definition(content))
        self.state.data["stage"] = "ready"
        self.state.save()
        return {"stage": "ready", "initial_cloud_consumer": summary, "pipeline": "Created; on-demand only, never scheduled.",
                "identity": "Notebook execution identity; not app-only least-privilege proof.",
                "consumer_config": {"tenant_id": self.config["tenant_id"],
                                    "graphql_endpoint": self.state.data["graphql"]["endpoint"],
                                    "authentication": "azure-cli"}}

    def intentional_run(self, name, run_id):
        if self.state.data.get("stage") != "ready":
            raise ValueError("Finish initial deployment before running intentional changes.")
        self.preflight()
        parameters = None
        if name == CONSUME:
            parameters = {"expected_publication_json": {
                "value": json.dumps(self.state.data["publication"], sort_keys=True), "type": "string",
            }}
        result = self.run(name, run_id, parameters)
        if name == PIPELINE:
            published, rows = self.graphql_client(
                self.state.data["graphql"]["endpoint"]
            ).verified_snapshot()
            self.state.data["publication"] = published
            self.state.save()
            return {"run_id": run_id, "status": result["status"],
                    "verified_api_rows": len(rows), "publication": published,
                    "note": "Pipeline is on-demand; no schedule was created."}
        output = self.exit_value(name, run_id, result)
        if name == PUBLISH:
            self.state.data["publication"] = output
            self.state.save()
        return {"run_id": run_id, "status": result["status"], "output": output}

    def retry_job(self, job_key, new_run_id):
        """An explicit new attempt, never an automatic resubmission of an old POST."""
        self.preflight(permit_manual_api=self.state.data.get("stage") == "waiting-for-graphql")
        original = self.state.data["jobs"].get(job_key)
        if not original or not new_run_id or job_key == new_run_id:
            raise ValueError("retry-job requires a tracked --job-key and a different --run-id.")
        if original["status"] == "Completed":
            replacement = self.state.data["jobs"].get(new_run_id)
            if replacement and replacement.get("id") == original.get("id"):
                return {"status": "Completed", "job_key": job_key, "run_id": new_run_id}
            raise ValueError("The tracked job already completed; it must not be retried.")
        if original["status"] not in TERMINAL_BAD:
            raise ValueError("Only terminal failed/cancelled jobs can be explicitly retried; resume active jobs instead.")
        if original["item_name"] == SIMULATE:
            raise ValueError("A failed simulation may already have committed. Inspect source counts before another intentional run.")
        parameters = original.get("parameters") or None
        result = self.run(original["item_name"], new_run_id, parameters)
        self.state.data.setdefault("retired_jobs", {}).setdefault(job_key, []).append(original)
        self.state.data["jobs"][job_key] = dict(self.state.data["jobs"][new_run_id])
        self.state.save()
        return {"status": result["status"], "job_key": job_key, "run_id": new_run_id,
                "next": "Resume the original prepare/complete command (or the original logical run-id)."}

    def retry_operation(self, resource_key):
        pending = self.state.data["operations"].get(resource_key)
        if not pending or pending.get("phase") not in ("rejected", "terminal_failed"):
            raise ValueError("Only a definitive HTTP rejection or terminal LRO failure can be reset. Ambiguous writes require reconcile.")
        if resource_key == "workspace":
            operation_kind, expected_path = "create", "/workspaces"
            if self.state.data.get("workspace"):
                raise ValueError("The workspace is already tracked; refusing to reset its creation intent.")
        elif resource_key.startswith("create:"):
            name = resource_key[len("create:"):]
            if name not in set(LAKEHOUSES) | set(NOTEBOOKS) | {PIPELINE}:
                raise ValueError("Unknown resource creation key.")
            if name in self.state.data["items"]:
                raise ValueError("The resource is already tracked; refusing to reset its creation intent.")
            suffix = "/lakehouses" if name in LAKEHOUSES else "/notebooks" if name in NOTEBOOKS else "/items"
            operation_kind, expected_path = "create", self.prefix() + suffix
        elif resource_key.startswith("update:"):
            name = resource_key[len("update:"):]
            tracked = self.state.data["items"].get(name)
            if name not in set(NOTEBOOKS) | {PIPELINE} or not tracked:
                raise ValueError("A definition-update reset requires the existing tracked notebook or pipeline.")
            operation_kind = "definition_update"
            expected_path = self.prefix() + "/items/" + guid(tracked["id"]) + "/updateDefinition"
        else:
            raise ValueError("Unknown saved operation key.")
        if pending.get("path") != expected_path:
            raise ValueError("Saved operation path does not match its tracked target; refusing to reset it.")
        self.preflight(permit_manual_api=self.state.data.get("stage") == "waiting-for-graphql")
        if operation_kind == "definition_update":
            self.require_idle()
        self.state.data.setdefault("retired_operations", []).append(dict(pending, key=resource_key))
        self.state.data["operations"].pop(resource_key)
        self.state.save()
        return {
            "reset_operation": resource_key, "operation_kind": operation_kind,
            "next": "Resume the original command with --apply after correcting the failure. No POST was sent by retry-operation.",
            "safety": (
                "Name/type guards confirmed the creation target is absent; any partially created resource must be reconciled instead."
                if operation_kind == "create" else
                "The existing resource and previous definition hash remain tracked. A new attempt can update only that same item ID."
            ),
        }

    def reconcile(self, resource_key=None, job_key=None, resource_id=None):
        """Bind a verified ID only to a saved ambiguous or terminal-failed creation intent."""
        resource_id = guid(resource_id)
        if job_key:
            job = self.state.data["jobs"].get(job_key)
            if not job or job.get("status") != "submitting":
                raise ValueError("Only an ambiguous, tracked job submission can be reconciled.")
            location = FABRIC + self.prefix() + "/items/" + job["item_id"] + "/jobs/instances/" + resource_id
            result = self.client.request("GET", location)[2]
            if result.get("id") != resource_id or result.get("itemId") != job["item_id"]:
                raise ValueError("Job does not match the pending item.")
            if job_timestamp(result["startTimeUtc"]) < job_timestamp(job["submitted_at"]):
                raise ValueError("Job predates the saved submission intent.")
            job.update(id=resource_id, location=location, status=result["status"], result=result)
            self.state.save()
            return {"reconciled_job": job_key, "next": "Resume the original command with the same run-id."}
        pending = self.state.data["operations"].get(resource_key)
        if not pending or pending.get("phase") not in ("submitting", "terminal_failed"):
            raise ValueError("Only an ambiguous or terminal-failed, tracked resource creation can be reconciled.")
        if resource_key == "workspace":
            item = self.client.request("GET", "/workspaces/" + resource_id)[2]
            self.validate_item(item, self.config["workspace_name"], "Workspace")
            if item["id"] != resource_id:
                raise ValueError("Workspace lookup did not return the verified resource ID.")
            if guid(item.get("capacityId")) != self.config["capacity_id"]:
                raise ValueError("Workspace is not on the selected capacity.")
            self.preflight(workspace_override=item)
            self.state.data["workspace"] = item
        elif resource_key.startswith("create:"):
            name = resource_key[len("create:"):]
            if name not in set(LAKEHOUSES) | set(NOTEBOOKS) | {PIPELINE}:
                raise ValueError("Unknown resource creation key.")
            kind = "Lakehouse" if name in LAKEHOUSES else "Notebook" if name in NOTEBOOKS else "DataPipeline"
            item = self.client.request("GET", self.prefix() + "/items/" + resource_id)[2]
            self.validate_item(item, name, kind)
            if item["id"] != resource_id:
                raise ValueError("Item lookup did not return the verified resource ID.")
            tracked = self.state.data["items"].get(name)
            if tracked and tracked["id"] != resource_id:
                raise ValueError("Cannot reconcile a different resource over an existing tracked item.")
            if kind == "Lakehouse":
                lakehouse = self.client.request("GET", self.prefix() + "/lakehouses/" + resource_id)[2]
                if lakehouse.get("properties", {}).get("defaultSchema") not in (None, "dbo"):
                    raise ValueError("Reconciled lakehouse has an unexpected default schema.")
            self.preflight(permit_manual_api=self.state.data.get("stage") == "waiting-for-graphql",
                           item_overrides={name: item})
            item["type"] = kind
            self.state.data["items"][name] = item
        else:
            raise ValueError(
                "A definition update is not a creation and cannot be reconciled to another ID. "
                "For a terminal failure, inspect the existing item then use retry-operation --resource-key KEY --apply. "
                "An update with an unknown outcome still requires operator inspection."
            )
        self.state.data.setdefault("reconciled_operations", []).append(dict(pending, key=resource_key, resource_id=resource_id))
        self.state.data["operations"].pop(resource_key)
        self.state.save()
        return {"reconciled_resource": resource_key, "next": "Resume the original command."}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", nargs="?", default="plan",
                        choices=["plan", "preflight", "prepare", "attach", "complete", "run",
                                 "status", "reconcile", "retry-job", "retry-operation"])
    parser.add_argument("--config", type=lambda value: ROOT / value, default=ROOT / "lab-config.json")
    parser.add_argument("--apply", action="store_true", help="Explicit consent for the selected stage's writes.")
    parser.add_argument("--endpoint")
    parser.add_argument("--confirm-read-only", action="store_true")
    parser.add_argument("--name", choices=[SEED, PUBLISH, SIMULATE, CONSUME, PIPELINE])
    parser.add_argument("--run-id", help="Stable ID for this intentional run; reuse it to resume, never to repeat.")
    parser.add_argument("--resource-key", help="Ambiguous operation key, e.g. create:FleetAlphaSource.")
    parser.add_argument("--job-key", help="Ambiguous run-id to reconcile.")
    parser.add_argument("--resource-id", help="Verified existing resource/job GUID; never selects by name.")
    args = parser.parse_args(argv)
    if args.operation == "plan" or (args.operation not in ("preflight", "status") and not args.apply):
        config = load_config(args.config) if args.config.exists() else None
        print(json.dumps(plan(config, args.operation), indent=2))
        return 0
    config = load_config(args.config)
    state = State(config)
    if args.operation == "status":
        print(json.dumps(state.data, indent=2))
        return 0
    auth = AzureCLIAuth(config["tenant_id"])
    client = FabricClient(auth, writes=args.apply, timeout=config["timeout_seconds"], poll=config["poll_seconds"])
    deployment = Deployment(config, state, client)
    if args.operation == "preflight":
        summary = deployment.preflight(permit_manual_api=state.data.get("stage") == "waiting-for-graphql")
    else:
        with state.lock():
            if args.operation not in ("reconcile", "retry-operation"):
                deployment.resume_pending()
            if args.operation == "prepare":
                summary = deployment.prepare()
            elif args.operation == "attach":
                summary = deployment.attach(args.endpoint or "", args.confirm_read_only)
            elif args.operation == "complete":
                summary = deployment.complete()
            elif args.operation == "run":
                if not args.name or not args.run_id:
                    raise ValueError("run requires --name and --run-id.")
                summary = deployment.intentional_run(args.name, args.run_id)
            elif args.operation == "retry-job":
                summary = deployment.retry_job(args.job_key, args.run_id)
            elif args.operation == "retry-operation":
                summary = deployment.retry_operation(args.resource_key)
            else:
                if bool(args.resource_key) == bool(args.job_key):
                    raise ValueError("reconcile requires exactly one of --resource-key or --job-key.")
                summary = deployment.reconcile(args.resource_key, args.job_key, args.resource_id)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, OSError, KeyError) as error:
        sys.exit(str(error))
