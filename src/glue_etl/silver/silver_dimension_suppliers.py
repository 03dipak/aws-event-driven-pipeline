import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from glue_etl.helpers import get_logger,read_iceberg,perform_merge
from glue_etl.helpers import ICEBERG_TABLE_SUPPLIERS,ICEBERG_SILVER_SUPPLIERS,BUCKET_NAME
from glue_etl.helpers.glue_config import configure_iceberg_spark_session

# ---------- Parse job arguments ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# ---------- Initialize Spark and Glue Context ----------
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initialize Logger
logger = get_logger(args['JOB_NAME'])

warehouse_path = f"s3://{BUCKET_NAME}/warehouse/"
# Set Iceberg configs
configure_iceberg_spark_session(spark, warehouse_path)

# ---------- run elt ----------
def run_etl():
    start_time = time.time()
    try:
        logger.info(f"Starting ETL Job {args['JOB_NAME']}")
        df_bronze = read_iceberg(spark,ICEBERG_TABLE_SUPPLIERS,False)
       
        df_bronze_cleaned = df_bronze.withColumn(
            "email", 
            F.when(F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), None)  # Remove non-numeric characters
            .otherwise(F.col("email"))
        )
        
        df_transformed = df_bronze_cleaned.select(
            F.col("supplier_id"),
            F.col("supplier_name"),
            F.col("contact_name"),
            F.col("email").cast("email"),
            F.col("phone"),
            F.col("address"),
            F.col("city"),
            F.col("state"),
            F.col("country"),
            F.col("postal_code"),
            F.col("business_category"),
            F.current_timestamp().alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(True).alias("is_current"),
            F.col("created_at"),
            F.col("updated_at")
        )
        
        df_target_supplier = read_iceberg(spark,ICEBERG_SILVER_SUPPLIERS,True)

        perform_merge(
            spark = spark,
            target_df = df_target_supplier, 
            source_df = df_transformed,
            target_view = "target_dim_supplier",
            source_view = "source_supplier",
            merge_conditions = "target.supplier_id = source.supplier_id AND target.is_current = True",
            update_condition = "target.supplier_name <> source.supplier_name OR target.contact_name <> source.contact_name OR target.email <> source.email OR target.phone <> source.phone OR target.address <> source.address OR target.city <> source.city OR target.state <> source.state OR target.country <> source.country OR target.postal_code <> source.postal_code OR target.business_category <> source.business_category",
            update_set = "target.is_current = False, target.end_date = current_timestamp()"
        )

    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
