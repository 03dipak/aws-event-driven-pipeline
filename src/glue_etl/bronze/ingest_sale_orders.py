import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from glue_etl.helpers import get_logger,get_new_files,read_csv,validate_data,deduplicate_data,write_to_iceberg,update_metadata
from glue_etl.helpers import DEDUPLICATE_KEYS_SALE_ORDERS,REQUIRED_COLUMNS_SALE_ORDERS,PARTITION_COLUMN_SALE_ORDERS,ICEBERG_TABLE_SALE_ORDERS,TABLE_NAME_SALE_ORDERS,SALE_ORDERS_SCHEMA,DEDUPLICATE_ORDERBY_CUSTOMERS

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

        file_keys = get_new_files(TABLE_NAME_SALE_ORDERS)
        for file_key in file_keys:
            try:
                df = read_csv(spark,SALE_ORDERS_SCHEMA,file_key)
                df = validate_data(df,REQUIRED_COLUMNS_SALE_ORDERS)
                df = deduplicate_data(df, DEDUPLICATE_KEYS_SALE_ORDERS, DEDUPLICATE_ORDERBY_CUSTOMERS)

                # Add partition columns
                df = df.withColumn("year", F.year(F.col(PARTITION_COLUMN_SALE_ORDERS))) \
                    .withColumn("month", F.month(F.col(PARTITION_COLUMN_SALE_ORDERS))) \
                    .withColumn("day", F.dayofmonth(F.col(PARTITION_COLUMN_SALE_ORDERS)))
                df = df.withColumn("ingestion_timestamp", F.current_timestamp())
                write_to_iceberg(df, ICEBERG_TABLE_SALE_ORDERS)
                update_metadata(file_key)

            except Exception as e:
                logger.error(f"ETL Job {args['JOB_NAME']} Failed to process {file_key}: {e}")
                raise e
    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
