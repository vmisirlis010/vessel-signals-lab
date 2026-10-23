if not spark.catalog.tableExists("dbo.measurements"):
    base = datetime(2026, 1, 1)
    changed = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for ship in range(1, 6):
        for tag_index, (tag_name, unit, typical) in enumerate(TAGS, start=1):
            for hour in range(24):
                measured = base + timedelta(hours=hour)
                vessel, tag_id = f"ALPHA-{ship:03d}", f"T{tag_index:02d}"
                reading_id = hashlib.sha256(f"{vessel}|{tag_id}|{measured.isoformat()}".encode()).hexdigest()[:32]
                value = round(typical * (1 + 0.01 * ship + 0.02 * math.sin(hour + tag_index)), 4)
                rows.append((reading_id, "Fleet Alpha", vessel, tag_id, tag_name, measured,
                             value, unit, "GOOD", changed, 1))
    spark.createDataFrame(rows, READING_SCHEMA).write.format("delta").mode("errorifexists").saveAsTable("dbo.measurements")
count = validate_source(spark.table("dbo.measurements"))
result = {"source_rows": count, "vessels": 5, "tags": 20, "synthetic_only": True}
print(json.dumps(result))
notebookutils.notebook.exit(json.dumps(result))
