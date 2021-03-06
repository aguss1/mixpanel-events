  
from pyspark.sql import Column, DataFrame, SparkSession, Window
from dbrix.utils.common import create_delta_table
from dbrix.utils.glue import create_or_update_glue_table
from dbrix.schemas.etl.mixpanel.processed import EventStruct
from dbrix.schemas.etl.mixpanel.processed import EventPropertiesStruct
import pyspark.sql.functions as F
import boto3
import datetime
from datetime import timedelta

def run(spark: SparkSession, warehouse_loc: str, task_env: str = "dev", sync_glue: bool = False, **kwargs):
    # Define full paths with warehouse_loc
   mixpanel_path = f"{warehouse_loc}/data/mixpanel/processed.delta"
   
# Create the mixpanel delta table if it does not exist
    create_delta_table(spark, mixpanel_path, EventStruct())

  mixpanel_df = (
        spark.read.format("delta")
        .load(mixpanel_path)
     
    )
  mixpanel_df.write.mode("overwrite").format("delta").save(mixpanel_path)
    if sync_glue and kwargs.get("disable_sandbox") and task_env != "test":
        create_or_update_glue_table(
            table_name="mixpanel",
            database_name=f"mixpanel_processed{task_env}".replace("_prod", ""),
            schema=EventStruct(),
            location=f"{mixpanel_path}/_symlink_format_manifest/",
        )

def get_session_start_df(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(path)
        .filter(F.col("customer_name").isNotNull())
        .withColumn("row_number", F.row_number().over(Window.partitionBy("customer_name").orderBy("event_timestamp")))
        .filter(_start_of_session("event_timestamp"))
        .select("customer_name", F.col("event_timestamp").alias("session_start"))
    )
  
 def get_session_end_df(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(path)
        .filter(F.col("customer_name").isNotNull())
        .withColumn("row_number", F.row_number().over(Window.partitionBy("customer_name").orderBy("event_timestamp")))
        .filter(_end_of_session("event_timestamp"))
        .select("customer_name", F.col("event_timestamp").alias("session_end"))
    )
 
def _start_of_session() -> Column:
  diff = datetime.datetime.strptime(F.col("event_timestamp"), '%Y-%m-%d %H:%M:%S.%f')
    - datetime.datetime.strptime(F.col("event_timestamp").shift(-1), '%Y-%m-%d %H:%M:%S.%f')
   return diff.minutes/60 > 20

def _end_of_session() -> Column:
  diff = datetime.datetime.strptime(F.col("event_timestamp").shift(1), '%Y-%m-%d %H:%M:%S.%f')
    - datetime.datetime.strptime(F.col("event_timestamp"), '%Y-%m-%d %H:%M:%S.%f')
   return diff.minutes/60 > 20

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analytic Campaigns ETL Task")
    parser.add_argument("-L", "--lake-loc", default="s3a://community-data-lake-dev")
    parser.add_argument("-W", "--warehouse-loc", default="s3a://community-data-warehouse-dev")
    parser.add_argument("-E", "--task-env", default="dev")
    parser.add_argument("-G", "--sync-glue", type=bool, default=False)
    parser.add_argument("--disable-sandbox", action="store_true", default=False)

    spark = SparkSession.builder.getOrCreate()
    run(spark, **parser.parse_args().__dict__)

