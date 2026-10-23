import ast
import base64
from copy import deepcopy
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from fabric_lab import Deployment
from fabric_runtime import FABRIC, FabricOperationFailed, State, digest
from lab_notebooks import (
    CONSUME, LAKEHOUSES, NOTEBOOKS, PIPELINE, PUBLISH, SEED,
    documents, notebook_definition, pipeline_content, write_generated,
)
from tests.support import FakeFabric, FakeGraphQL, ROOT, config, endpoint, publication, scratch, uid


class DeploymentTests(TestCase):
    def deployment(self, directory):
        state = State(config(), directory / "state.json")
        return Deployment(config(), state, FakeFabric())

    def prepare(self, deployment, directory):
        with patch("fabric_lab.write_generated") as generated, patch("fabric_lab.time.sleep"):
            result = deployment.prepare()
        return result

    def test_preflight_reads_only_and_checks_active_capacity_and_collisions(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            self.assertFalse(deployment.preflight()["cloud_writes"])
            self.assertFalse(deployment.state.path.exists())
            self.assertTrue(all(call[0].startswith("GET") for call in deployment.client.calls))
            deployment.client.capacity_state = "Paused"
            with self.assertRaises(ValueError):
                deployment.preflight()
            deployment.client.capacity_state = "Active"
            deployment.client.workspaces = [{"id": uid("other"), "displayName": config()["workspace_name"]}]
            with self.assertRaises(ValueError):
                deployment.preflight()

    def test_prepare_attach_complete_repeat_and_manual_boundaries(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            instructions = self.prepare(deployment, directory)
            self.assertEqual(deployment.state.data["stage"], "waiting-for-graphql")
            self.assertIn("MANUAL", instructions["stage"])
            self.assertNotIn(CONSUME, deployment.state.data["items"])
            self.assertEqual(len(deployment.state.data["items"]), 6)
            posts = [call for call in deployment.client.calls if call[0] == "POST"]
            self.prepare(deployment, directory)
            self.assertEqual(len([call for call in deployment.client.calls if call[0] == "POST"]), len(posts))
            with self.assertRaises(ValueError):
                deployment.complete()
            deployment.client.items.append({
                "id": uid("api"), "displayName": config()["graphql_api_name"], "type": "GraphQLApi",
            })
            with patch.object(deployment, "graphql_client", return_value=FakeGraphQL().client()):
                with self.assertRaises(ValueError):
                    deployment.attach(endpoint(), False)
                with self.assertRaises(ValueError):
                    deployment.attach(endpoint(workspace=uid("other-workspace")), True)
                attached = deployment.attach(endpoint(), True)
                self.assertEqual(attached["verified_rows"], 1440)
                with patch("fabric_lab.write_generated"), patch("fabric_lab.time.sleep"):
                    completed = deployment.complete()
                self.assertEqual(completed["stage"], "ready")
                self.assertIn(PIPELINE, deployment.state.data["items"])
                count = len([call for call in deployment.client.calls if call[0] == "POST"])
                with patch("fabric_lab.write_generated"), patch("fabric_lab.time.sleep"):
                    deployment.complete()
                self.assertEqual(len([call for call in deployment.client.calls if call[0] == "POST"]), count)
            self.assertEqual(len(deployment.client.jobs), 3)
            with patch.object(deployment, "graphql_client", return_value=FakeGraphQL().client()), \
                    patch("fabric_lab.time.sleep"):
                cycle = deployment.intentional_run(PIPELINE, "publish-collect-1")
            self.assertEqual(cycle["verified_api_rows"], 1440)
            self.assertEqual(deployment.state.data["publication"], cycle["publication"])

    def test_pending_accepted_workspace_result_is_bound_before_collision_check(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            workspace = {"id": uid("workspace"), "displayName": config()["workspace_name"],
                         "capacityId": uid("capacity"), "type": "Workspace"}
            deployment.client.workspaces.append(workspace)
            deployment.state.data["operations"]["workspace"] = {
                "phase": "accepted", "response": (201, {}, workspace), "expect_result": True,
            }
            deployment.state.save()
            deployment.resume_pending()
            deployment.preflight()
            self.assertEqual(deployment.state.data["workspace"]["id"], uid("workspace"))
            self.assertFalse(deployment.state.data["operations"])
            self.assertFalse(any(call[0] == "POST" for call in deployment.client.calls))

    def test_untracked_name_type_and_changed_capacity_are_refused(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            self.prepare(deployment, directory)
            item = deployment.client.items[0]
            old = item["id"]
            item["id"] = uid("untracked")
            with self.assertRaises(ValueError):
                deployment.preflight()
            item["id"] = old
            item["type"] = "Notebook"
            with self.assertRaises(ValueError):
                deployment.preflight()
            item["type"] = "Lakehouse"
            deployment.client.workspaces[0]["capacityId"] = uid("another-capacity")
            with self.assertRaises(ValueError):
                deployment.preflight()

    def test_job_duplicate_run_id_payload_and_active_job_guards(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            self.prepare(deployment, directory)
            count = len([call for call in deployment.client.calls if call[0] == "POST"])
            self.assertEqual(deployment.run(SEED, "initial:seed")["status"], "Completed")
            self.assertEqual(len([call for call in deployment.client.calls if call[0] == "POST"]), count)
            with self.assertRaises(ValueError):
                deployment.run(PUBLISH, "initial:seed")
            with self.assertRaises(ValueError):
                deployment.run(SEED, "new-id-too-soon")
            deployment.state.data["jobs"]["unknown"] = {"status": "submitting"}
            with self.assertRaises(ValueError):
                deployment.require_idle()
            deployment.state.data["jobs"]["unknown"] = {
                "status": "submitting", "fingerprint": digest({
                    "item_id": deployment.state.data["items"][SEED]["id"], "parameters": {},
                }),
            }
            with self.assertRaises(RuntimeError):
                deployment.run(SEED, "unknown")

    def test_job_timeout_persists_id_and_only_polls_on_resume(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            self.prepare(deployment, directory)
            job = deployment.state.data["jobs"]["initial:seed"]
            job["status"] = "InProgress"
            job["retry_after"] = 61
            count = len([call for call in deployment.client.calls if call[0] == "POST"])
            with self.assertRaises(TimeoutError):
                deployment.run(SEED, "initial:seed")
            self.assertIn("id", State(config(), deployment.state.path).data["jobs"]["initial:seed"])
            job["retry_after"] = 0
            with patch("fabric_lab.time.sleep"):
                deployment.run(SEED, "initial:seed")
            self.assertEqual(len([call for call in deployment.client.calls if call[0] == "POST"]), count)

    def test_explicit_failed_job_retry_requires_new_id_and_rebinds_logical_checkpoint(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            self.prepare(deployment, directory)
            job = deployment.state.data["jobs"]["initial:seed"]
            job["status"] = "Failed"
            deployment.client.jobs[job["id"]].update(status="Failed", startTimeUtc="2000-01-01T00:00:00")
            with self.assertRaises(ValueError):
                deployment.retry_job("initial:seed", "initial:seed")
            with patch("fabric_lab.time.sleep"):
                result = deployment.retry_job("initial:seed", "seed-recovery-1")
            self.assertEqual(result["status"], "Completed")
            self.assertEqual(deployment.state.data["jobs"]["initial:seed"]["status"], "Completed")
            self.assertEqual(len(deployment.state.data["retired_jobs"]["initial:seed"]), 1)
            self.assertEqual(deployment.retry_job("initial:seed", "seed-recovery-1")["status"], "Completed")

    def test_rejected_operation_reset_never_clears_ambiguous_writes_or_posts(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            deployment.state.data["operations"]["workspace"] = {"phase": "submitting"}
            with self.assertRaises(ValueError):
                deployment.retry_operation("workspace")
            deployment.state.data["operations"]["workspace"] = {"phase": "rejected", "http_status": 400, "path": "/workspaces"}
            deployment.retry_operation("workspace")
            self.assertNotIn("workspace", deployment.state.data["operations"])
            self.assertFalse(any(call[0] == "POST" for call in deployment.client.calls))

    def failed_intent(self, path, definition=None, status="Failed"):
        return {
            "phase": "terminal_failed", "terminal_status": status, "path": path,
            "polling_location": FABRIC + "/operations/" + uid("failed-op"),
            "definition_hash": digest(definition) if definition else None,
        }

    def test_native_same_name_sql_endpoints_allow_next_create_and_preflight(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            deployment.ensure_workspace()
            source = deployment.ensure_item(LAKEHOUSES[0], "Lakehouse")
            self.assertEqual([item["type"] for item in deployment.client.items], ["Lakehouse", "SQLEndpoint"])
            deployment.preflight()
            deployment.ensure_item(LAKEHOUSES[1], "Lakehouse")
            deployment.preflight()
            self.assertEqual(len(deployment.state.data["items"]), 2)
            self.assertEqual(len(deployment.client.items), 4)
            self.assertTrue(any(
                call[0] == "GET" and call[1] == deployment.prefix() + "/lakehouses/" + source["id"]
                for call in deployment.client.calls
            ))

    def test_unrelated_duplicate_or_unverifiable_sql_companions_are_never_ignored(self):
        for mode in ("unrelated", "duplicate-endpoint", "wrong-kind", "duplicate-lakehouse", "extra-notebook", "unresolved", "pending"):
            with self.subTest(mode=mode), scratch() as directory:
                deployment = self.deployment(directory)
                deployment.ensure_workspace()
                source = deployment.ensure_item(LAKEHOUSES[0], "Lakehouse")
                endpoint_item = deployment.client.items[1]
                if mode == "unrelated":
                    endpoint_item["id"] = uid("unrelated-endpoint")
                elif mode == "duplicate-endpoint":
                    deployment.client.items.append(dict(endpoint_item, id=uid("duplicate-endpoint")))
                elif mode == "wrong-kind":
                    deployment.client.items[0]["type"] = "Notebook"
                elif mode == "duplicate-lakehouse":
                    deployment.client.items.append(dict(source, id=uid("duplicate-lakehouse")))
                elif mode == "extra-notebook":
                    deployment.client.items.append({"id": uid("extra-notebook"), "displayName": source["displayName"], "type": "Notebook"})
                elif mode == "unresolved":
                    deployment.client.lakehouse_properties[source["id"]]["sqlEndpointProperties"].pop("id")
                else:
                    deployment.client.lakehouse_properties[source["id"]]["sqlEndpointProperties"] = None
                posts = len([call for call in deployment.client.calls if call[0] == "POST"])
                with self.assertRaises(ValueError):
                    deployment.preflight()
                with self.assertRaises(ValueError):
                    deployment.ensure_item(LAKEHOUSES[1], "Lakehouse")
                self.assertEqual(len([call for call in deployment.client.calls if call[0] == "POST"]), posts)

    def test_terminal_create_failure_can_be_reset_only_when_target_is_absent(self):
        for status in ("Failed", "Cancelled"):
            with self.subTest(status=status), scratch() as directory:
                deployment = self.deployment(directory)
                deployment.state.data["operations"]["workspace"] = self.failed_intent("/workspaces", status=status)
                deployment.state.save()
                with self.assertRaises(FabricOperationFailed):
                    deployment.resume_pending()
                result = deployment.retry_operation("workspace")
                self.assertEqual(result["operation_kind"], "create")
                self.assertFalse(deployment.state.data["operations"])
                self.assertEqual(deployment.state.data["retired_operations"][0]["terminal_status"], status)
                self.assertFalse(any(call[0] == "POST" for call in deployment.client.calls))
                deployment.ensure_workspace()
                self.assertEqual(len(deployment.client.workspaces), 1)

    def test_partial_failed_workspace_creation_must_be_reconciled_not_recreated(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            workspace = {"id": uid("workspace"), "displayName": config()["workspace_name"],
                         "capacityId": uid("capacity"), "type": "Workspace"}
            deployment.client.workspaces.append(workspace)
            deployment.state.data["operations"]["workspace"] = self.failed_intent("/workspaces")
            deployment.state.save()
            with self.assertRaises(ValueError):
                deployment.retry_operation("workspace")
            deployment.reconcile(resource_key="workspace", resource_id=workspace["id"])
            self.assertEqual(deployment.state.data["workspace"]["id"], workspace["id"])
            self.assertFalse(deployment.state.data["operations"])
            deployment.ensure_workspace()
            self.assertFalse(any(call[0] == "POST" for call in deployment.client.calls))

    def test_failed_workspace_reconciliation_refuses_ambiguous_names(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            workspace = {"id": uid("workspace"), "displayName": config()["workspace_name"],
                         "capacityId": uid("capacity"), "type": "Workspace"}
            deployment.client.workspaces.extend([workspace, dict(workspace, id=uid("other-workspace"))])
            deployment.state.data["operations"]["workspace"] = self.failed_intent("/workspaces")
            with self.assertRaises(ValueError):
                deployment.reconcile(resource_key="workspace", resource_id=workspace["id"])
            self.assertNotIn("workspace", deployment.state.data)
            self.assertIn("workspace", deployment.state.data["operations"])

    def test_partial_failed_lakehouse_creation_validates_companion_and_collision_on_reconcile(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            deployment.ensure_workspace()
            source = deployment.client.request(
                "POST", deployment.prefix() + "/lakehouses", {"displayName": LAKEHOUSES[0]}
            )[2]
            key = "create:" + LAKEHOUSES[0]
            deployment.state.data["operations"][key] = self.failed_intent(deployment.prefix() + "/lakehouses")
            deployment.state.save()
            posts = len([call for call in deployment.client.calls if call[0] == "POST"])
            with self.assertRaises(ValueError):
                deployment.retry_operation(key)
            extra = {"id": uid("unrelated-endpoint"), "displayName": source["displayName"], "type": "SQLEndpoint"}
            deployment.client.items.append(extra)
            with self.assertRaises(ValueError):
                deployment.reconcile(resource_key=key, resource_id=source["id"])
            self.assertNotIn(LAKEHOUSES[0], deployment.state.data["items"])
            self.assertIn(key, deployment.state.data["operations"])
            deployment.client.items.remove(extra)
            deployment.reconcile(resource_key=key, resource_id=source["id"])
            deployment.ensure_item(LAKEHOUSES[0], "Lakehouse")
            deployment.preflight()
            self.assertEqual(len([call for call in deployment.client.calls if call[0] == "POST"]), posts)
            self.assertEqual(len(deployment.client.items), 2)

    def test_terminal_definition_update_reset_preserves_item_and_retries_only_same_id(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            self.prepare(deployment, directory)
            item = deployment.state.data["items"][SEED]
            original_hash, original_id = item["definition_hash"], item["id"]
            item_count = len(deployment.client.items)
            key = "update:" + SEED
            path = deployment.prefix() + "/items/" + original_id + "/updateDefinition"
            definition = {"parts": [{"path": "notebook-content.ipynb", "payload": "changed"}]}
            deployment.state.data["operations"][key] = self.failed_intent(path, definition, "Cancelled")
            deployment.state.save()
            with self.assertRaises(ValueError):
                deployment.reconcile(resource_key=key, resource_id=original_id)
            with self.assertRaises(FabricOperationFailed):
                deployment.resume_pending()
            posts = len([call for call in deployment.client.calls if call[0] == "POST"])
            result = deployment.retry_operation(key)
            self.assertEqual(result["operation_kind"], "definition_update")
            self.assertEqual(item["definition_hash"], original_hash)
            self.assertEqual(item["id"], original_id)
            self.assertEqual(len([call for call in deployment.client.calls if call[0] == "POST"]), posts)
            deployment.ensure_item(SEED, "Notebook", definition)
            self.assertEqual(deployment.client.calls[-1][1], path)
            self.assertEqual(item["definition_hash"], digest(definition))
            self.assertEqual(len(deployment.client.items), item_count)

    def test_failed_update_reset_requires_matching_tracked_id_and_rejects_name_collisions(self):
        with scratch() as directory:
            deployment = self.deployment(directory)
            self.prepare(deployment, directory)
            item = deployment.state.data["items"][SEED]
            key = "update:" + SEED
            path = deployment.prefix() + "/items/" + item["id"] + "/updateDefinition"
            deployment.state.data["operations"][key] = self.failed_intent(
                deployment.prefix() + "/items/" + uid("untracked-notebook") + "/updateDefinition"
            )
            with self.assertRaises(ValueError):
                deployment.retry_operation(key)
            deployment.state.data["operations"][key] = self.failed_intent(path)
            deployment.state.data["jobs"]["active-other-run"] = {"status": "InProgress"}
            with self.assertRaises(ValueError):
                deployment.retry_operation(key)
            deployment.state.data["jobs"].pop("active-other-run")
            deployment.client.items.append({"id": uid("collision"), "displayName": SEED, "type": "Notebook"})
            with self.assertRaises(ValueError):
                deployment.retry_operation(key)
            self.assertIn(key, deployment.state.data["operations"])


class ArtifactTests(TestCase):
    def fixture(self):
        return {
            "tenant_id": uid("tenant"), "workspace": {"id": uid("workspace")}, "graphql": {"endpoint": endpoint()},
            "items": {name: {"id": uid(name), "displayName": name} for name in LAKEHOUSES + tuple(NOTEBOOKS)},
            "publication": publication(),
        }

    def test_metadata_parameter_cells_payload_and_no_bundled_identity(self):
        state = self.fixture()
        docs = documents(state, include_consumer=True)
        self.assertEqual(set(docs), set(NOTEBOOKS))
        for name, document in docs.items():
            with self.subTest(name=name):
                metadata = document["metadata"]["dependencies"]["lakehouse"]
                self.assertEqual(metadata["default_lakehouse"], uid(NOTEBOOKS[name]))
                self.assertEqual(metadata["default_lakehouse_workspace_id"], uid("workspace"))
                self.assertEqual(document["cells"][0]["metadata"]["tags"], ["parameters"])
                for cell in document["cells"]:
                    self.assertTrue(all(line.endswith("\n") for line in cell["source"]))
                    ast.parse("".join(cell["source"]))
                    self.assertEqual(cell["outputs"], [])
                encoded = notebook_definition(document)
                decoded = json.loads(base64.b64decode(encoded["parts"][0]["payload"]))
                self.assertEqual(decoded, document)
        text = json.dumps(docs)
        self.assertNotIn("certificate_thumbprint", text)
        self.assertNotIn("consumer-identity.json", text)
        self.assertNotIn(".zip", text)
        publisher = "".join("".join(cell["source"]) for cell in docs[PUBLISH]["cells"])
        self.assertIn("trident.onelake.endpoint", publisher)
        self.assertIn("Tables/dbo/measurements", publisher)
        consumer = "".join("".join(cell["source"]) for cell in docs[CONSUME]["cells"])
        self.assertNotIn("abfss://", consumer)
        self.assertIn('getToken("pbi")', consumer)

    def test_pipeline_uses_resolved_notebooks_no_schedule_or_fixed_wait(self):
        content = pipeline_content(self.fixture())
        activities = content["properties"]["activities"]
        self.assertEqual(len(activities), 2)
        self.assertTrue(all(item["type"] == "TridentNotebook" for item in activities))
        self.assertEqual(activities[0]["typeProperties"]["notebookId"], uid(PUBLISH))
        self.assertEqual(activities[1]["typeProperties"]["notebookId"], uid(CONSUME))
        self.assertEqual(activities[1]["typeProperties"]["workspaceId"], uid("workspace"))
        self.assertIn("exitValue", json.dumps(activities[1]["typeProperties"]["parameters"]))
        self.assertNotIn("schedule", json.dumps(content).lower())
        self.assertEqual(activities[1]["policy"]["retry"], 0)

    def test_generated_files_only_use_allowed_names_and_ignored_output(self):
        with scratch() as directory:
            path = write_generated(SEED + ".ipynb", {"test": True}, directory)
            self.assertTrue(path.exists())
            with self.assertRaises(ValueError):
                write_generated("../README.md", {}, directory)
        ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
        for rule in (".vessel-signals/", "lab-config.json", "consumer-config.json",
                     "consumer-identity.json", "*.ipynb", "*.pfx", "*.sqlite", "*.zip"):
            self.assertIn(rule, ignore.splitlines())

    def test_public_code_and_templates_have_no_environment_specific_values(self):
        prohibited = (
            r"C:\\Users\\", "certificate_expires_utc", "wabi-", "westus", "west-us",
            "accessToken\": \"", "Bearer eyJ",
        )
        for path in list(ROOT.glob("*.py")) + list((ROOT / "notebook_sources").glob("*.py")) + list(ROOT.glob("*.example.json")):
            text = path.read_text(encoding="utf-8")
            for value in prohibited:
                with self.subTest(path=path.name, value=value):
                    self.assertNotIn(value, text)
        # All GUIDs in published code must be obtained from config/discovery, not literals.
        import re
        for path in list(ROOT.glob("*.py")) + list((ROOT / "notebook_sources").glob("*.py")):
            self.assertIsNone(re.search(r"\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b",
                                       path.read_text(encoding="utf-8")), path.name)

    def test_optional_certificate_wrapper_never_exports_tokens_or_registers_apps(self):
        script = (ROOT / "Invoke-CertificateConsumer.ps1").read_text(encoding="utf-8")
        self.assertIn("$env:VSL_ACCESS_TOKEN = $response.access_token", script)
        self.assertIn("$env:VSL_ACCESS_TOKEN = $previousToken", script)
        self.assertIn("$Operation -eq 'sync' -and -not $Apply", script)
        self.assertNotIn("Write-Output $response", script)
        self.assertNotIn("New-SelfSignedCertificate", script)
        self.assertNotIn("graph.microsoft.com", script)
        self.assertNotIn("applications", script)

    def test_ci_is_offline_and_runs_on_windows_and_linux(self):
        workflow = (ROOT / ".github" / "workflows" / "checks.yml").read_text(encoding="utf-8")
        for expected in (
            "ubuntu-latest", "windows-latest", '"3.11"', '"3.12"', "contents: read",
            "persist-credentials: false", "python -m compileall",
            "python -B -m unittest discover -s tests -v", "python -B fabric_lab.py plan",
        ):
            self.assertIn(expected, workflow)
        for forbidden in ("azure/login", "secrets.", "--apply", "az login", "id-token: write"):
            self.assertNotIn(forbidden, workflow)
