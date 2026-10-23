from pyspark.sql.types import LongType

onelake = notebookutils.conf.get("trident.onelake.endpoint").replace("https://", "").rstrip("/")
source_path = f"abfss://{workspace_id}@{onelake}/{source_lakehouse_id}/Tables/dbo/measurements"
raw = spark.read.format("delta").load(source_path)
source_count = validate_source(raw)
approved_vessels = [f"ALPHA-{number:03d}" for number in range(1, 5)]
approved_tags = [f"T{number:02d}" for number in range(1, 16)]
approved = raw.filter(
    F.col("vessel_id").isin(approved_vessels) & F.col("tag_id").isin(approved_tags)
).select(*[field.name for field in READING_SCHEMA]).cache()
count = approved.count()
if count != source_count // 100 * 60 or count < 1440:
    raise ValueError("Approved export count does not match the source grid.")
if approved.select("reading_id").distinct().count() != count:
    raise ValueError("Duplicate approved IDs.")
if approved.filter(
    ~F.col("vessel_id").isin(approved_vessels) | ~F.col("tag_id").isin(approved_tags)
).count():
    raise ValueError("Export is outside the approved scope.")
approved.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.measurements")
published = datetime.now(timezone.utc).replace(tzinfo=None)
upper = approved.agg(F.max("modified_at_utc")).first()[0]
status_schema = StructType([
    StructField("dataset_id", StringType(), False),
    StructField("provider", StringType(), False),
    StructField("consumer", StringType(), False),
    StructField("published_at_utc", TimestampType(), False),
    StructField("upper_modified_at_utc", TimestampType(), False),
    StructField("row_count", LongType(), False),
    StructField("vessel_count", LongType(), False),
    StructField("tag_count", LongType(), False),
])
values = ("vessel-signals-v1", "Fleet Alpha", "Analytics Beta", published, upper, count, 4, 15)
spark.createDataFrame([values], status_schema).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("dbo.publication_status")
approved.unpersist()
result = dict(zip([field.name for field in status_schema], values))
for key in ("published_at_utc", "upper_modified_at_utc"):
    result[key] = result[key].replace(tzinfo=timezone.utc).isoformat(timespec="microseconds")
print(json.dumps(result))
notebookutils.notebook.exit(json.dumps(result))
