import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = "gold_daily_city_air_quality",
    comment = "Daily aggregated air quality metrics by city and pollutant"
)
def gold_daily_city_air_quality():

    df = spark.readStream.table("silver_measurements")

    agg_df = df.groupBy("city", "state", "country","parameter", "event_date")\
               .agg(
                   avg("value").alias("avg_value"),
                   min("value").alias("min_value"),
                   max("value").alias("max_value"),
                   count("value").alias("measurement_count")
               )\
                .withColumn("created_at", current_timestamp())
    
    return agg_df

@dlt.table(
    name = "gold_daily_sensor_health",
    comment = "Daily sensor-level data coverage and air quality metrics"
)
def gold_daily_sensor_health():
    df = spark.readStream.table("silver_measurements")

    agg_df = df.groupBy("sensor_id", "city", "state", "parameter")\
               .agg(avg("value").alias("avg_value"),
                    min("value").alias("min_value"),
                    max("value").alias("max_value"),
                    count("value").alias("readings_count"))\
               .withColumn("created_at", current_timestamp())
    
    return agg_df
               

@dlt.table(
    name = "gold_dim_current_sensors",
    comment = "Current active sensor dimension (flattened from SCD2)"
)

def gold_dim_current_sensors():
    sensors_df = spark.readStream.table("silver_sensors")

    latest_current_records = sensors_df.filter(
        col("__END_AT").isNull()
    )\
        .select(
            "sensor_id",
            "sensor_name",
            "parameter_name",
            "parameter_units",
            "location_id",
            "city",
            "state",
            "country",
            col("__START_AT").alias("effective_from"),
            current_timestamp().alias("created_at")
        )
    
    return latest_current_records






