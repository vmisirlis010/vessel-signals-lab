import hashlib
import json
import math
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")
READING_SCHEMA = StructType([
    StructField("reading_id", StringType(), False),
    StructField("provider", StringType(), False),
    StructField("vessel_id", StringType(), False),
    StructField("tag_id", StringType(), False),
    StructField("tag_name", StringType(), False),
    StructField("measured_at_utc", TimestampType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), False),
    StructField("quality", StringType(), False),
    StructField("modified_at_utc", TimestampType(), False),
    StructField("revision", IntegerType(), False),
])
TAGS = [
    ("speed_over_ground", "knots", 12.0), ("fuel_rate", "kg/h", 1800.0),
    ("engine_rpm", "rpm", 75.0), ("shaft_power", "kW", 8200.0),
    ("draft_forward", "m", 12.0), ("draft_aft", "m", 12.2),
    ("sea_temperature", "degC", 22.0), ("air_temperature", "degC", 24.0),
    ("wind_speed", "m/s", 6.0), ("heading", "deg", 125.0),
    ("distance_log", "nm", 380.0), ("generator_power", "kW", 750.0),
    ("coolant_temperature", "degC", 78.0), ("exhaust_temperature", "degC", 340.0),
    ("lubrication_pressure", "bar", 4.2),
    ("internal_auxiliary_01", "unit", 10.0), ("internal_auxiliary_02", "unit", 20.0),
    ("internal_auxiliary_03", "unit", 30.0), ("internal_auxiliary_04", "unit", 40.0),
    ("internal_auxiliary_05", "unit", 50.0),
]


def validate_source(frame):
    count = frame.count()
    if count < 2400 or (count - 2400) % 100:
        raise ValueError("Source must have 2,400 rows plus complete 100-row hours.")
    if frame.select("reading_id").distinct().count() != count:
        raise ValueError("Duplicate source measurement IDs.")
    if frame.filter(
        (F.col("provider") != "Fleet Alpha")
        | ~F.col("vessel_id").isin([f"ALPHA-{n:03d}" for n in range(1, 6)])
        | ~F.col("tag_id").isin([f"T{n:02d}" for n in range(1, 21)])
        | (F.col("revision") < 1)
    ).count():
        raise ValueError("Unexpected source scope.")
    if frame.select("vessel_id").distinct().count() != 5 or frame.select("tag_id").distinct().count() != 20:
        raise ValueError("Source must have five vessels and twenty tags.")
    groups = frame.groupBy("vessel_id", "tag_id").count()
    if groups.count() != 100 or groups.filter(F.col("count") != count // 100).count():
        raise ValueError("Incomplete source vessel/tag grid.")
    return count
