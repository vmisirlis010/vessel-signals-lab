Deploy Vessel Signals Lab
=========================

Deployment model
----------------

The supported path is a **guarded two-phase deployment with a guided GraphQL
checkpoint**, not an unattended one-click installation.

The Python CLI provisions the source/export/landing Lakehouses, generates
deployment-bound notebooks, seeds and publishes the synthetic baseline,
validates your configured GraphQL API, and finishes the consumer and pipeline.
It never creates capacity, adopts an unrelated existing project, grants access,
registers an application or creates a schedule.

The reference screenshots show an earlier working deployment. The portable
deployment has four executable notebooks and a **two-activity** pipeline.
The earlier ``00_Start_Here`` runbook, connector bundle and fixed Wait activity
are replaced by repository documentation, the repository's standalone connector
and bounded publication-marker polling.

Prerequisites
-------------

* Python 3.10 or later, Azure CLI, and permission to authenticate interactively
  to your chosen tenant. No third-party Python packages are required locally.
* A Fabric-enabled tenant, an existing **active** capacity visible to you, and
  permission to create a dedicated workspace and assign it to that capacity.
* Permission to create Lakehouses, notebooks, a pipeline and an API for GraphQL
  and to run the required Spark jobs.
* Tenant/region support for the required Fabric features. The CLI can require
  the Notebook job-instance query API with ``beta=true`` to obtain a
  publisher's exit value when the core scheduler response does not contain it.
  This preview dependency is not a long-term compatibility guarantee.
* A new, dedicated workspace name. Existing untracked workspaces and item-name
  collisions are rejected rather than silently reused.

The active capacity can incur charges. The lab does not resume, resize, pause
or delete capacity. Obtain approval for compute usage before running any
``--apply`` command.

1. Configure and inspect the plan
--------------------------------

From the repository root in PowerShell:

.. code-block:: powershell

   Copy-Item .\lab-config.example.json .\lab-config.json

Edit ``lab-config.json``:

.. code-block:: json

   {
     "tenant_id": "YOUR-TENANT-GUID",
     "capacity_id": "YOUR-EXISTING-ACTIVE-CAPACITY-GUID",
     "workspace_name": "Vessel Signals Lab Public",
     "graphql_api_name": "AnalyticsBetaTelemetryAPI",
     "timeout_seconds": 1800,
     "poll_seconds": 15
   }

Replace both GUID placeholders and choose a workspace name not used by another
project. A subscription ID is not needed. Keep this file local: it is ignored
by Git.

.. code-block:: powershell

   python fabric_lab.py
   az login --tenant YOUR-TENANT-GUID --allow-no-subscriptions
   python fabric_lab.py preflight

The default command is an **offline plan**: no authentication, cloud calls or
local state writes. ``preflight`` makes cloud reads only, verifies the
authenticated tenant and selected active capacity, and checks naming
collisions.

All cloud-writing commands show a plan unless ``--apply`` is supplied.
For a nondefault configuration, add ``--config "PATH-TO-CONFIG"`` to each CLI
command. The single local state file is bound to the full configuration:
use a separate checkout for a separate lab rather than swapping configurations
against one state file.

2. Prepare the synthetic source and export
------------------------------------------

After reviewing and approving the plan:

.. code-block:: powershell

   python fabric_lab.py prepare --apply

This creates a tracked dedicated workspace, three schema-enabled Lakehouses
and the seed, publish and simulation notebooks. It runs the initial seed and
publication jobs, saving resource and operation identifiers locally.

The baseline is 2,400 source rows and 1,440 approved rows. Setup **does not run
the change simulator**.

Generated deployment files live under ``.vessel-signals/generated``.
Tracked state lives in ``.vessel-signals/state.json``. Both are ignored.
Preserve them to resume safely. A successful ``prepare`` stops at the GraphQL
checkpoint; it is not a complete end-to-end deployment.

3. Create the read-only GraphQL API
----------------------------------

Use the workspace URL and instructions printed by ``prepare``:

1. Create a new **API for GraphQL** using the configured API name.
2. Choose **SSO** and only the SQL analytics endpoint belonging to
   **AnalyticsBetaExport**. Do not select FleetAlphaSource or the consumer
   landing Lakehouse.
3. Wait for SQL table discovery, then select only ``dbo.measurements`` and
   ``dbo.publication_status``.
4. If prompted for keys, choose ``reading_id`` for measurements and
   ``dataset_id`` for publication status.
5. Keep the default query names ``measurements`` and
   ``publication_statuses``. Disable all mutation operations, if any.
6. Use **Copy endpoint** in the API toolbar. The browser's page URL is not the
   GraphQL endpoint.

Lakehouse SQL analytics endpoints are read-only. Still inspect the API schema
and do not add a writable data source.

This checkpoint uses documented Fabric UI behavior instead of unsupported
internal authoring endpoints. An agent can guide it, but must not claim this
part is automatically configured by ``prepare``.

.. code-block:: powershell

   python fabric_lab.py attach --endpoint "COPIED-ENDPOINT" --confirm-read-only --apply

Attach verifies the HTTPS endpoint, workspace/API identity, expected read-only
schema, exact publisher marker, complete row count and allowed vessel/tag scope.
``--confirm-read-only`` is the operator's confirmation of the UI configuration,
not an instruction to bypass validation.

SQL visibility can lag. If validation reports stale or incomplete data, allow
synchronization to finish and repeat attach. Do not seed another workspace,
relax the scope checks or invent a publication result to get past the error.

4. Finish the consumer and pipeline
-----------------------------------

.. code-block:: powershell

   python fabric_lab.py complete --apply
   python fabric_lab.py status

Complete deploys and runs the HTTP consumer notebook and creates the
**unscheduled** ``Publish_And_Collect`` pipeline.

The pipeline publishes, then invokes the consumer with the publisher's exact
exit marker. The consumer polls successful publication-status reads every
15 seconds for up to 600 seconds. It does not assume that a fixed sleep has
made all data current.

Inspect actual job outcomes and consumer data. In a fresh baseline, expect
1,440 unique approved readings; ALPHA-005 and tags T16-T20 must be absent.
Local unit tests and the recorded screenshots are not live deployment proof.

The notebook uses its execution identity. This easy path does not create a
standalone application or prove least-privilege partner isolation.

Run the change demonstration
---------------------------

The simulator is intentionally mutating. Run it only when another synthetic
hour and a correction are wanted:

.. code-block:: powershell

   python fabric_lab.py run --name 03_Simulate_New_And_Corrected_Readings --run-id hour-1 --apply
   python fabric_lab.py run --name Publish_And_Collect --run-id cycle-1 --apply

One simulation adds 100 source rows, including 60 approved rows, and corrects
one existing approved reading. A fresh lab then has 2,500 source rows and
1,500 distinct approved rows.

**The run ID is an idempotency key for this local deployment.** Reuse the same
ID to resume or inspect that run. Choose a new ID only to request new
intentional work. The CLI rejects accidental submissions when the item has
another run within the last five minutes; wait rather than bypassing the guard.

To demonstrate consumer replay without new publication:

.. code-block:: powershell

   python fabric_lab.py run --name 04_Consumer_API_Connector --run-id replay-1 --apply

Wait for any recent-job guard to expire before a genuinely new run. No duplicate
readings should be created.

Supported run names are ``01_Seed_Synthetic_Telemetry``,
``02_Publish_Approved_Data``, ``03_Simulate_New_And_Corrected_Readings``,
``04_Consumer_API_Connector`` and ``Publish_And_Collect``.

Serialize publication and collection. The publication data and status tables
are separate Delta writes, so exact-marker polling is **not** an atomic
multi-table snapshot guarantee.

Optional standalone SQLite consumer
------------------------------------

Start with ``consumer-config.example.json`` and create the ignored
``consumer-config.json`` with your tenant and attached endpoint. The
``azure-cli`` authentication mode uses your signed-in operator identity; it is
not an app-only isolation demonstration.

.. code-block:: powershell

   python consumer.py check
   python consumer.py sync
   python consumer.py sync --apply

``check`` performs reads. ``sync`` without ``--apply`` prints a plan.
An applied sync persists to ``consumer-output`` by default, committing
SQLite data and checkpoint together. ``--config PATH`` and ``--database PATH``
override the local input and output locations.

For a separately onboarded Windows certificate-backed application:

* Copy ``consumer-identity.example.json`` to ``consumer-identity.json`` and
  supply the actual tenant, client ID and certificate thumbprint.
* Set ``consumer-config.json`` authentication to ``environment`` and supply
  the matching ``client_id``.
* The certificate must already exist with a usable private key in the current
  Windows user's certificate store. Its public credential must already be
  registered on the application.
* Grant only the required API Read/Execute and export SQL read permissions;
  do not grant a workspace role, provider source access or provider write access.
  Onboarding and permissions require their own explicit approval and are
  outside the automatic deployment.

.. code-block:: powershell

   .\Invoke-CertificateConsumer.ps1 -Operation check
   .\Invoke-CertificateConsumer.ps1 -Operation sync -Apply

The wrapper never registers applications, changes access grants or prints
tokens. It passes the token to its child process only. Do not commit local
identity files or certificate material.

Resume and recover
------------------

**Timeout or lost polling connection**
   Repeat the original command with the same configuration and run ID.
   Accepted operations are polled rather than resubmitted.

**A job definitively failed**
   Inspect its actual Fabric error first. For example, after correcting the
   initial consumer failure:

   .. code-block:: powershell

      python fabric_lab.py retry-job --job-key initial:consume --run-id consume-recovery-1 --apply
      python fabric_lab.py complete --apply

**A write was rejected or an asynchronous operation terminally failed**
   Inspect the terminal failure and correct its cause, then explicitly reset
   that saved intent and resume the original stage:

   .. code-block:: powershell

      python fabric_lab.py retry-operation --resource-key KEY --apply

   The reset itself sends no POST. A creation reset is allowed only when the
   target is absent; a partially created resource must be reconciled instead.
   A failed definition update can only be retried against its existing tracked
   item while jobs are idle. Recovery history is retained in the local state.

**A submission outcome is ambiguous**
   Inspect Fabric read-only and reconcile only a verified matching resource or
   job. Never guess an identifier:

   .. code-block:: powershell

      python fabric_lab.py reconcile --resource-key KEY --resource-id VERIFIED-GUID --apply
      python fabric_lab.py reconcile --job-key RUN-ID --resource-id VERIFIED-JOB-GUID --apply

Definition updates with an unknown outcome deliberately halt for operator
inspection; they cannot be adopted as newly created resources.
Do not delete state to bypass an ambiguity, collision or changed-config error.
If a process leaves a lock after an abrupt termination, first establish that
no deployment process still owns it before any manual recovery.

Troubleshooting
---------------

* **Wrong tenant or authentication failure:** sign in interactively to the
  explicit target tenant. Never paste a token into chat or configuration.
* **Capacity inaccessible or inactive:** select an authorized, existing active
  capacity. The script does not activate it.
* **Name already used:** choose a new dedicated workspace, unless resuming the
  same tracked deployment. Do not adopt an unrelated project.
* **Tables not visible in GraphQL:** wait for SQL analytics synchronization;
  verify the selected export SQL endpoint and exact two tables.
* **Schema or mutation validation failed:** correct the API configuration.
  Do not disable validation or expose the source.
* **Publication result unavailable:** inspect notebook execution and tenant
  support for the Notebook job-instance query API, including its preview
  dependency. Do not infer success from item existence.
* **Freshness timeout or count mismatch:** prevent concurrent publishers,
  inspect SQL visibility, then resume the original failed work deliberately.
* **Five-minute recent-job guard:** wait before an intentional new run; reuse
  the original run ID if you are only trying to resume it.

Do not create a schedule, delete the workspace or change capacity lifecycle
as an automatic recovery action.

Local development
-----------------

.. code-block:: powershell

   python -m unittest discover -s tests -v
   python fabric_lab.py

CI runs offline checks on Windows and Ubuntu with Python 3.11 and 3.12.
It needs no Azure secrets and never deploys cloud resources. A real cloud
deployment still requires separate authorization and end-to-end inspection.

References
----------

* `Create a GraphQL API and add data <https://learn.microsoft.com/fabric/data-engineering/get-started-api-graphql>`_
* `GraphQL schema view <https://learn.microsoft.com/fabric/data-engineering/graphql-schema-view>`_
* `Fabric REST APIs <https://learn.microsoft.com/rest/api/fabric/articles/>`_
* `Fabric Notebook public APIs <https://learn.microsoft.com/fabric/data-engineering/notebook-public-api>`_
