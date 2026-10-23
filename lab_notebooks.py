"""Generate identifier-bound Fabric notebooks only into ignored local output."""

import base64
import json
from pathlib import Path

from fabric_runtime import ROOT

SEED = "01_Seed_Synthetic_Telemetry"
PUBLISH = "02_Publish_Approved_Data"
SIMULATE = "03_Simulate_New_And_Corrected_Readings"
CONSUME = "04_Consumer_API_Connector"
PIPELINE = "Publish_And_Collect"
LAKEHOUSES = ("FleetAlphaSource", "AnalyticsBetaExport", "AnalyticsBetaLanding")
NOTEBOOKS = {SEED: LAKEHOUSES[0], PUBLISH: LAKEHOUSES[1], SIMULATE: LAKEHOUSES[0], CONSUME: LAKEHOUSES[2]}


def code(text, parameters=False):
    return {
        "cell_type": "code", "metadata": {"tags": ["parameters"]} if parameters else {},
        "source": [line + "\n" for line in text.rstrip("\n").split("\n")],
        "execution_count": None, "outputs": [],
    }


def notebook(cells, lakehouse, workspace_id):
    return {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark", "language": "python", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
            "dependencies": {"lakehouse": {
                "default_lakehouse": lakehouse["id"],
                "default_lakehouse_name": lakehouse["displayName"],
                "default_lakehouse_workspace_id": workspace_id,
                "known_lakehouses": [{"id": lakehouse["id"]}],
            }},
        },
        "cells": cells,
    }


def source(name):
    return (ROOT / "notebook_sources" / (name + ".py")).read_text(encoding="utf-8")


def documents(state, include_consumer=False):
    workspace = state["workspace"]["id"]
    common = source("common")
    result = {}
    for name, file in ((SEED, "seed"), (PUBLISH, "publish"), (SIMULATE, "simulate")):
        parameters = f"workspace_id = {workspace!r}\n"
        if name == PUBLISH:
            parameters += f"source_lakehouse_id = {state['items']['FleetAlphaSource']['id']!r}\n"
        result[name] = notebook(
            [code(parameters, parameters=True), code(common), code(source(file))],
            state["items"][NOTEBOOKS[name]], workspace,
        )
    if include_consumer:
        expected = state["publication"]
        parameters = (
            f"workspace_id = {workspace!r}\n"
            f"tenant_id = {state['tenant_id']!r}\n"
            f"graphql_endpoint = {state['graphql']['endpoint']!r}\n"
            f"expected_publication_json = {json.dumps(expected, sort_keys=True)!r}\n"
        )
        result[CONSUME] = notebook(
            [code(parameters, parameters=True), code(common),
             code((ROOT / "connector_core.py").read_text(encoding="utf-8")), code(source("consume"))],
            state["items"][NOTEBOOKS[CONSUME]], workspace,
        )
    return result


def notebook_definition(document):
    return {"format": "ipynb", "parts": [{
        "path": "notebook-content.ipynb", "payloadType": "InlineBase64",
        "payload": base64.b64encode(json.dumps(document).encode()).decode("ascii"),
    }]}


def pipeline_content(state):
    workspace = state["workspace"]["id"]
    activities = []
    for name, notebook_name, dependencies in (
        ("Publish_Approved_Data", PUBLISH, []),
        ("Collect_Through_API", CONSUME, [{"activity": "Publish_Approved_Data", "dependencyConditions": ["Succeeded"]}]),
    ):
        properties = {"notebookId": state["items"][notebook_name]["id"], "workspaceId": workspace}
        if notebook_name == CONSUME:
            properties["parameters"] = {
                "expected_publication_json": {
                    "value": {"value": "@activity('Publish_Approved_Data').output.result.exitValue", "type": "Expression"},
                    "type": "string",
                }
            }
        activities.append({
            "name": name, "type": "TridentNotebook", "dependsOn": dependencies,
            "policy": {"timeout": "0.01:00:00", "retry": 0, "retryIntervalInSeconds": 30},
            "typeProperties": properties,
        })
    return {"properties": {"activities": activities}}


def pipeline_definition(content):
    return {"parts": [{
        "path": "pipeline-content.json", "payloadType": "InlineBase64",
        "payload": base64.b64encode(json.dumps(content).encode()).decode("ascii"),
    }]}


def write_generated(name, value, directory=None):
    directory = Path(directory) if directory else ROOT / ".vessel-signals" / "generated"
    directory.mkdir(parents=True, exist_ok=True)
    if name not in {f"{item}.ipynb" for item in NOTEBOOKS} | {"pipeline-content.json"}:
        raise ValueError("Unknown generated artifact name.")
    path = directory / name
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return path
