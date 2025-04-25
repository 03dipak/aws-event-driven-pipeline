import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from glue_etl.helpers import get_logger,read_iceberg,perform_merge
from glue_etl.helpers import ICEBERG_TABLE_CUSTOMERS,ICEBERG_SILVER_CUSTOMERS

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
        df_bronze = read_iceberg(spark,ICEBERG_TABLE_CUSTOMERS,False)
       
        df_bronze_cleaned = df_bronze.withColumn(
            "PostalCode", 
            F.when(F.col("PostalCode").rlike("[^0-9]"), None)  # Remove non-numeric characters
            .otherwise(F.col("PostalCode"))
        )
        
        df_transformed = df_bronze_cleaned.select(
            F.col("CustomerID").alias("customer_id").cast("int"),
            F.concat_ws(" ", F.col("FirstName"), F.col("LastName")).alias("customer_name"),
            F.col("EmailAddress").alias("email").cast("email"),
            F.col("Phone").alias("phone"),
            F.col("City").alias("region"),
            F.col("PostalCode").alias("postal_code"),
            F.col("CountryRegion").alias("country"),
            F.col("CompanyName").alias("company_name"),
            F.col("AddressLine1").alias("address_line"),
            F.current_timestamp().alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(True).alias("is_current"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        )
        
        df_target_customer = read_iceberg(spark,ICEBERG_SILVER_CUSTOMERS,True)

        perform_merge(
            spark = spark,
            target_df = df_target_customer, 
            source_df = df_transformed,
            target_view = "target_dim_customer",
            source_view = "source_customer",
            merge_conditions = "target.customer_id = source.customer_id AND target.is_current = True",
            update_condition = "target.email != source.email OR target.phone != source.phone OR target.customer_name != source.customer_name",
            update_set = "target.is_current = False, target.end_date = current_timestamp(),target.updated_at = current_timestamp()"
        )
        
    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
