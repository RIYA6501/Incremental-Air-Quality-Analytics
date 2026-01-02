import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "bronze_locations",
    comment = "Flattened OpenAQ locations data"
)
@dlt.expect("valid_location_id", "location_id IS NOT NULL")
@dlt.expect("valid_coordinates", "latitude IS NOT NULL AND longitude IS NOT NULL")

def bronze_locations():

    raw_schema = StructType([
        StructField("ingestiontime", StringType()),
        StructField("source", StringType()),
        StructField("results", ArrayType(
            StructType([
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("isMobile", BooleanType()),
                StructField("isMonitor", BooleanType()),
                StructField("timezone", StringType()),
                StructField("coordinates", StructType([
                    StructField("latitude", DoubleType()),
                    StructField("longitude", DoubleType())
                ])),
                StructField("country", StructType([
                    StructField("code", StringType()),
                    StructField("name", StringType())
                ]))
            ])
        ))
    ])

    raw_df = spark.readStream.schema(raw_schema)\
        .option("maxFilesPerTrigger", 50)\
        .json("/Volumes/air_quality/openaq/ingestion/locations/")

    df = raw_df.selectExpr("ingestiontime", "explode(results) as loc")

    return (
        df.select(
            col("loc.id").alias("location_id"),
            col("loc.name").alias("location_name"),
            col("loc.country.code").alias("country_code"),
            col("loc.country.name").alias("country_name"),
            col("loc.coordinates.latitude").alias("latitude"),
            col("loc.coordinates.longitude").alias("longitude"),
            col("loc.timezone").alias("timezone"),
            col("loc.isMobile").alias("is_mobile"),
            col("loc.isMonitor").alias("is_monitor"),
            col("ingestiontime"),
            current_timestamp().alias("ingested_at"))\
        .dropDuplicates(["location_id"])
        
    )

@dlt.table(
    name = "bronze_sensors",
    comment = "Sensors extracted from openaq (INDIA) locations"
)
@dlt.expect("valid_sensor_id", "sensor_id IS NOT NULL")
def bronze_sensors():

    raw_schema = StructType([
        StructField("ingestiontime", StringType()),
        StructField("source", StringType()),
        StructField("results", ArrayType(
            StructType([
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("isMobile", BooleanType()),
                StructField("isMonitor", BooleanType()),
                StructField("timezone", StringType()),
                StructField("country", StructType([
                    StructField("code", StringType())
                ])),
                StructField("sensors", ArrayType(
                    StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                        StructField("parameter", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                            StructField("units", StringType())
                        ]))
                    ])
                ))
            ])
        ))
    ])

    raw_df = spark.readStream.schema(raw_schema)\
        .option("maxFilesPerTrigger", 50)\
        .json("/Volumes/air_quality/openaq/ingestion/locations/")

    base_df = raw_df.selectExpr("ingestiontime","explode(results) as loc")

    sensors_exploded_df = base_df.select(
    col("loc.id").alias("location_id"),
    col("loc.name").alias("location_name"),
    col("loc.country.code").alias("country_code"),
    col("loc.timezone").alias("timezone"),
    col("loc.isMobile").alias("is_mobile"),
    col("loc.isMonitor").alias("is_monitor"),
    explode("loc.sensors").alias("sensor"),
    col("ingestiontime")
)

    return (
        sensors_exploded_df
    .select(
        col("sensor.id").alias("sensor_id"),
        col("sensor.name").alias("sensor_name"),
        col("sensor.parameter.id").alias("parameter_id"),
        col("sensor.parameter.name").alias("parameter_name"),
        col("sensor.parameter.units").alias("parameter_units"),
        col("location_id"),
        col("location_name"),
        col("country_code"),
        col("timezone"),
        col("is_mobile"),
        col("is_monitor"),
        col("ingestiontime"),
        current_timestamp().alias("bronze_processed_ts")
    )
    .dropDuplicates(["sensor_id"])
)

@dlt.table(
    name = "bronze_measurements",
    comment = "Daily air quality measurements from OpenAQ"
)
@dlt.expect("valid_value", "value IS NOT NULL")
@dlt.expect("valid_parameter", "parameter IS NOT NULL")

def bronze_measurements():

    schema = StructType([
        StructField("datetime_utc", StringType()),
        StructField("event_date", DateType()),
        StructField("ingestion_time", StringType()),
        StructField("parameter", StringType()),
        StructField("sensor_id", LongType()),
        StructField("source", StringType()),
        StructField("unit", StringType()),
        StructField("value", DoubleType())
    ])
    df = spark.readStream.schema(schema)\
        .option("maxFilesPerTrigger", 50)\
        .json("/Volumes/air_quality/openaq/ingestion/measurements/")

    return (
        df\
        .withColumn("ingested_at", current_timestamp())
        .dropDuplicates([
            "sensor_id",
            "parameter",
            "datetime_utc"
        ])
    )