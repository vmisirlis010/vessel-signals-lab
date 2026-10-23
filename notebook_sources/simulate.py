from delta.tables import DeltaTable

existing = spark.table("dbo.measurements")
before = validate_source(existing)
latest_time = existing.agg(F.max("measured_at_utc")).first()[0]
latest_rows = existing.filter(F.col("measured_at_utc") == latest_time).collect()
if len(latest_rows) != 100:
    raise ValueError("The latest synthetic hour is incomplete.")
last_modified = existing.agg(F.max("modified_at_utc")).first()[0]
changed = max(datetime.now(timezone.utc).replace(tzinfo=None), last_modified + timedelta(microseconds=1))
new_time = latest_time + timedelta(hours=1)
new_rows = []
for row in latest_rows:
    value = row.asDict()
    value["reading_id"] = hashlib.sha256(
        f'{value["vessel_id"]}|{value["tag_id"]}|{new_time.isoformat()}'.encode()
    ).hexdigest()[:32]
    value.update(measured_at_utc=new_time, modified_at_utc=changed,
                 value=round(value["value"] * 1.001, 4), revision=1)
    new_rows.append(value)
correction = existing.filter(
    (F.col("vessel_id") == "ALPHA-001") & (F.col("tag_id") == "T01")
).orderBy("measured_at_utc").first().asDict()
correction.update(value=round(correction["value"] + 0.25, 4),
                  revision=correction["revision"] + 1,
                  modified_at_utc=changed, quality="CORRECTED")
# One Delta commit avoids appending an hour without its corresponding correction.
incoming = spark.createDataFrame(new_rows + [correction], existing.schema)
DeltaTable.forName(spark, "dbo.measurements").alias("target").merge(
    incoming.alias("source"), "target.reading_id = source.reading_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
after = validate_source(spark.table("dbo.measurements"))
if after != before + 100:
    raise ValueError("Simulation did not add exactly 100 source rows.")
result = {"source_rows": after, "source_readings_added": 100,
          "expected_export_readings_added": 60, "existing_readings_corrected": 1}
print(json.dumps(result))
notebookutils.notebook.exit(json.dumps(result))
