# Agent instructions: Vessel Signals Lab

## Goal

Deploy this synthetic demonstration into the user's explicitly selected
Microsoft Fabric tenant, using an existing active capacity. Preserve the
provider/export/API/consumer boundaries. Do not modify an existing deployment
merely because its names look familiar.

Read README.rst, docs/deployment.rst and docs/architecture.rst first. The
deployment guide is the command reference; use the repository's supported CLI
rather than reconstructing REST calls or inventing deployment endpoints.

All organizations, vessels and telemetry are fictitious. Never replace the
synthetic data with a real customer dataset to make the demo more convincing.

## Ask only for the missing decisions

1. Target tenant ID.
2. Existing active Fabric capacity ID the user is authorized to use.
3. A dedicated, unambiguous workspace name.
4. Explicit approval for the deployment plan and use of paid compute.

Discover available capacities read-only if the user asks for help selecting
one. Never create, resize, resume, pause or delete capacity. A deployment
approval is not permission to change billing or tenant-wide settings.

Do not guess tenant IDs, copy identifiers from screenshots, or reuse another
operator's configuration. If the CLI identity is signed into the wrong tenant,
stop for interactive authentication. Never collect the user's password, ask
for access tokens in chat, or write tokens into files.

## Prepare without cloud writes

- Confirm Python 3.10 or later and Azure CLI are available. Core local Python
  code uses the standard library; do not add packages merely to run it.
- Inspect the example configuration and create the ignored local configuration
  with the user's approved values.
- Use the documented planning/preflight command first. Resolve errors rather
  than substituting a default capacity, workspace or identity.
- Explain what will be created, the identity that will execute notebooks, the
  GraphQL configuration checkpoint and the cost implications.
- Obtain explicit approval before any cloud write or Spark job.

## Deploy in resumable stages

Follow the exact commands in docs/deployment.rst:

1. Provision the tracked workspace, three schema-enabled Lakehouses and source
   notebooks. Bind generated notebooks to the discovered deployment IDs.
2. Seed and publish the synthetic baseline. Do not run the change simulator as
   part of setup.
3. Complete the documented GraphQL checkpoint using only supported public
   interfaces. Use AnalyticsBetaExport, and expose only dbo.measurements and
   dbo.publication_status. Disable all mutation operations.
4. Attach and validate the user's actual HTTPS Fabric GraphQL endpoint. Never
   paste a reference endpoint or quietly accept another workspace's API.
5. Deploy the API consumer and on-demand pipeline, then run the documented
   initial collection and verification.

If a public API does not support a required GraphQL configuration operation,
guide the user through the Fabric UI instead of calling undocumented browser
endpoints. Browser assistance still requires approval for the proposed writes.
Never claim the manual checkpoint is fully automated.

Keep the generated deployment state. Resume with the same approved config and
state; do not delete state to bypass a collision guard. If a POST times out or a
job's outcome is unknown, inspect tracked operation/job identifiers and current
service state before any retry. Never submit a duplicate notebook or pipeline
run just because polling is slow.

## Acceptance criteria

- The source initially contains 2,400 readings from five vessels and twenty
  tags; the fresh export and consumer each contain 1,440 approved readings.
- The consumer retrieves through GraphQL, not direct provider Lakehouse reads.
- ALPHA-005 and tags T16-T20 are absent from the approved export and consumer.
- Reading IDs are unique.
- Running the consumer again without new publication changes creates no
  duplicate readings.
- The API requires authentication and has no mutation fields.
- Only after separate approval, one change simulation adds 100 source readings,
  60 approved readings and one correction. After publication and collection,
  there are 1,500 distinct approved readings in a fresh baseline exercise.
  Repeated simulations intentionally add further hours; do not expect a fixed
  total in a previously used lab.

Record actual operation outcomes. A local unit test, compiled notebook, old
screenshot or successful item creation is not proof of a successful cloud
data flow. If an end-to-end run was not performed, state that explicitly.

## Identity and consistency limits

The Fabric consumer uses its notebook execution identity. Do not describe that
as a proven least-privilege partner service principal. Optional standalone
certificate-app onboarding is a separate, explicitly approved operation.
Never grant a consumer workspace Contributor or source access as a shortcut.

Serialize publication and collection in this demonstration. SQL visibility
is asynchronous. The portable pipeline passes the exact publisher marker to
the consumer, which polls for up to 600 seconds; the recorded screenshot's
fixed Wait activity is not part of the portable pipeline. Separate publication
data/status tables are not an atomic API snapshot. The SQLite connector's
transaction is not a Fabric cross-table transaction.

The CLI can require the Notebook job-instance query API with beta=true to read
the publisher's exit value when the core scheduler response lacks it. If that
preview API is unavailable, report the blocker; do not invent a publication
marker, bypass freshness checks, or claim success from item creation alone.

Do not claim support for hard deletes, shipboard ingestion, CDC, API Management,
cross-tenant onboarding, production recovery or a recurring schedule.

## Development and publication hygiene

Run the repository's offline tests after code changes. Do not use a live
deployment as a test environment without separate approval.

Never commit local configuration/state, token output, app identities,
certificate material, SQLite databases, generated run evidence, notebooks with
private outputs, dependency caches or real deployment endpoints.

Do not enable automatic cloud deployment in GitHub Actions. CI must be offline
and must not require Azure credentials.

When handing off, give the actual workspace link, which stages completed,
which identity was used, any remaining manual checkpoint and whether capacity
was left running. Do not create schedules or send notifications unless asked.
