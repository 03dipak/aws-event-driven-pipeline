import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from glue_etl.helpers import get_logger,read_iceberg,perform_merge
from glue_etl.helpers import ICEBERG_TABLE_PRODUCTS,ICEBERG_SILVER_PRODUCTS,BUCKET_NAME
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
        df_bronze = read_iceberg(spark,ICEBERG_TABLE_PRODUCTS,False)
       
        df_bronze_cleaned = df_bronze.withColumn(
            "ProductNumber", 
            F.when(F.col("ProductNumber").rlike("[^0-9]"), None)  # Remove non-numeric characters
            .otherwise(F.col("ProductNumber"))
        )
        
        df_transformed = df_bronze_cleaned.select(
            F.col("ProductID").alias("product_id").cast("int"),
            F.col("Name").alias("product_name"),
            F.col("ProductNumber").alias("product_number").cast("int"),
            F.col("ProductCategoryID").alias("category"),
            F.col("ProductModelID").alias("sub_category"),
            F.col("Color").alias("color"),
            F.col("Size").alias("size"),
            F.col("Weight").alias("weight"),
            F.col("ListPrice").alias("unit_price").cast("double"),
            F.col("StandardCost").alias("cost_price").cast("double"),
            F.col("SellStartDate").alias("sell_start_date").cast("timestamp"),
            F.col("ThumbNailPhoto").alias("thumbnail_photo"),
            F.col("ThumbnailPhotoFileName").alias("thumbnail_photo_file_name"),
            F.col("rowguid").alias("rowguid"),
            F.current_timestamp().alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(True).alias("is_current"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        )
        
        df_target_product = read_iceberg(spark,ICEBERG_SILVER_PRODUCTS,True)

        perform_merge(
            spark = spark,
            target_df = df_target_product, 
            source_df = df_transformed,
            target_view = "target_dim_product",
            source_view = "source_product",
            merge_conditions = "target.product_id = source.product_id AND target.is_current = True",
            update_condition = "target.product_name <> source.product_name OR target.category <> source.category OR target.sub_category <> source.sub_category OR target.unit_price <> source.unit_price OR target.cost_price <> source.cost_price",
            update_set = "target.is_current = False, target.end_date = current_timestamp(),     target.updated_at = current_timestamp()"
        )

    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
