import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from glue_etl.helpers import get_logger,read_iceberg,perform_merge
from glue_etl.helpers import ICEBERG_TABLE_WAREHOUSES,ICEBERG_SILVER_WAREHOUSES

# ---------- Parse job arguments ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# ---------- Initialize Spark and Glue Context ----------
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initialize Logger
logger = get_logger(args['JOB_NAME'])
# ---------- run elt ----------
def run_etl():
    start_time = time.time()
    try:
        logger.info(f"Starting ETL Job {args['JOB_NAME']}")
        df_bronze = read_iceberg(spark,ICEBERG_TABLE_WAREHOUSES,False)
       
        df_bronze_cleaned = df_bronze.withColumn(
            "PostalCode", 
            F.when(F.col("PostalCode").rlike("[^0-9]"), None)  # Remove non-numeric characters
            .otherwise(F.col("PostalCode"))
        )
        
        df_transformed = df_bronze_cleaned.select(
            F.col("warehouse_id"),
            F.col("warehouse_name"),
            F.col("location"),
            F.col("city"),
            F.col("state"),
            F.col("country"),
            F.col("postal_code").cast("int"),
            F.col("capacity").cast("int"),
            F.current_timestamp().alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(True).alias("is_current"),
            F.col("created_at"),
            F.col("updated_at")
        )
        
        df_target_warehouse = read_iceberg(spark,ICEBERG_SILVER_WAREHOUSES,True)

        perform_merge(
            spark = spark,
            target_df = df_target_warehouse, 
            source_df = df_transformed,
            target_view = "target_dim_warehouse",
            source_view = "source_warehouse",
            merge_conditions = "target.warehouse_id = source.warehouse_id AND target.is_current = True",
            update_condition = "target.location <> source.location",
            update_set = "target.is_current = False, target.end_date = current_timestamp()"
        )

    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
