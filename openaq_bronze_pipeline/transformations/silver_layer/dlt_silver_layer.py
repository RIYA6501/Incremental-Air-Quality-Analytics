import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = "silver_locations",
    comment = "locations silver table"
)
def silver_locations():
    locations_df = spark.readStream.table("bronze_locations")
    city_look_up_df = dlt.read("air_quality.reference.city_lookup")

    join_cols = ["latitude", "longitude"]
    join_df = locations_df.join(city_look_up_df, join_cols, "left")
    return join_df

# Tracking SCD Type 2 changes for Sensors Data.

@dlt.view(
    name = "sensors_silver_view",
    comment = "View for Tracking SCD2 Type changes for Sensors Data"
)

def sensors_silver_view():
    sensors_df = spark.readStream.table("bronze_sensors")
    locations_silver_df = spark.read.table("silver_locations")

    join_df = sensors_df.join(
        locations_silver_df.select("location_id", "city", "state", "country"), 
        "location_id",
        "left"
    )\
        .select(
            "sensor_id",
            "sensor_name",
            "parameter_id",
            "parameter_name",
            "parameter_units",
            "location_id",
            "is_mobile",
            "is_monitor",
            "timezone",
            "city",
            "state",
            "country",
            current_timestamp().alias("update_ts")
        )
    
    return join_df

#  Creating Silver Table for SCD-2 change history.

dlt.create_streaming_table(
    name="silver_sensors",
    comment="SCD Type 2 table for sensors"
)

dlt.create_auto_cdc_flow(
    target = "silver_sensors",
    source = "sensors_silver_view",
    keys = ["sensor_id"],
    sequence_by = "update_ts",
    stored_as_scd_type = 2
)

@dlt.table(
    name = "silver_measurements",
    comment= "measurements silver table"
)
def silver_measurements():
    measures_df = spark.readStream.table("bronze_measurements")
    sensors_silver_df = spark.read.table("silver_sensors")
    join_df = measures_df.join(sensors_silver_df, "sensor_id", "left") \
        .select(
            "sensor_id",
            "city",
            "state",
            "country",
            "parameter",
            "value",
            "unit",
            to_timestamp("datetime_utc").alias("event_ts"),
            "event_date"
        )
    return join_df