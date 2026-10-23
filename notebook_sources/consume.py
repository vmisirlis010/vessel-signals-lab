from functools import reduce
from delta.tables import DeltaTable


def execution_token():
    token = notebookutils.credentials.getToken("pbi")
    try:
        import base64
        encoded = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        valid = str(UUID(claims["tid"])) == tenant_id and claims["exp"] > time.time() + 30
    except (KeyError, ValueError, IndexError, TypeError):
        valid = False
    if not valid:
        raise ValueError("Notebook token tenant or expiry differs from deployment. Token suppressed.")
    return token


client = GraphQLClient(graphql_endpoint, execution_token)
if client.workspace_id != workspace_id:
    raise ValueError("Consumer endpoint workspace differs from its deployment.")
client.verify_read_only()
expected = json.loads(expected_publication_json) if expected_publication_json else None
if not expected:
    raise ValueError("A publisher's exact exit value is required; a fixed sleep does not establish freshness.")
status = client.wait_for_publication(expected, timeout_seconds=600, poll_seconds=15)
lower = None
if spark.catalog.tableExists("dbo.consumer_checkpoint"):
    checkpoint = spark.table("dbo.consumer_checkpoint").collect()
    if len(checkpoint) != 1 or checkpoint[0]["endpoint"] != graphql_endpoint:
        raise ValueError("Unexpected consumer checkpoint.")
    lower = checkpoint[0]["watermark"]
upper = status["upper_modified_at_utc"]
rows, page_count = [], 0
for page in client.pages(lower, upper):
    rows.extend(page)
    page_count += 1
client.unchanged(status)
if rows:
    for row in rows:
        row["measured_at_utc"] = timestamp(row["measured_at_utc"]).replace(tzinfo=None)
        row["modified_at_utc"] = timestamp(row["modified_at_utc"]).replace(tzinfo=None)
        row["value"] = float(row["value"])
    incoming = spark.createDataFrame(rows, READING_SCHEMA)
    if spark.catalog.tableExists("dbo.received_measurements"):
        target = spark.table("dbo.received_measurements").alias("target")
        joined = incoming.alias("source").join(target, "reading_id", "inner")
        differences = reduce(
            lambda a, b: a | b,
            [~F.col(f"source.{field.name}").eqNullSafe(F.col(f"target.{field.name}"))
             for field in READING_SCHEMA if field.name != "reading_id"],
        )
        invalid = (
            (F.col("source.revision") < F.col("target.revision"))
            | (F.col("source.modified_at_utc") < F.col("target.modified_at_utc"))
            | ((F.col("source.revision") == F.col("target.revision")) & differences)
        )
        if joined.filter(invalid).count():
            raise ValueError("Incoming data would regress a stored reading or change it without a revision.")
        DeltaTable.forName(spark, "dbo.received_measurements").alias("target").merge(
            incoming.alias("source"), "target.reading_id = source.reading_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        incoming.write.format("delta").mode("errorifexists").saveAsTable("dbo.received_measurements")
if not spark.catalog.tableExists("dbo.received_measurements"):
    raise ValueError("No initial consumer data.")
received = spark.table("dbo.received_measurements")
actual = received.count()
if actual != status["row_count"] or received.select("reading_id").distinct().count() != actual:
    raise ValueError("Landing rows/count differ from publication; checkpoint not advanced.")
if received.filter(
    (F.col("provider") != "Fleet Alpha")
    | ~F.col("vessel_id").isin(sorted(ALLOWED_VESSELS))
    | ~F.col("tag_id").isin(sorted(ALLOWED_TAGS))
).count():
    raise ValueError("Landing data is outside approved scope.")
client.unchanged(status)
# Separate Delta tables are not a transaction. Replaying the merge is safe if checkpoint writing fails.
spark.createDataFrame(
    [(graphql_endpoint, upper, datetime.now(timezone.utc).replace(tzinfo=None), len(rows))],
    "endpoint string, watermark string, completed_at_utc timestamp, fetched_rows long",
).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.consumer_checkpoint")
result = {"fetched_rows": len(rows), "pages": page_count, "stored_readings": actual, "watermark": upper}
print(json.dumps(result))
notebookutils.notebook.exit(json.dumps(result))
