import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from glue_etl.helpers import get_logger,read_iceberg,deduplicate_data,perform_merge
from glue_etl.helpers import ICEBERG_TABLE_SALE_ORDERS,ICEBERG_SILVER_SALE_ORDERS,BUCKET_NAME
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
        df_bronze = read_iceberg(spark,ICEBERG_TABLE_SALE_ORDERS,False)
       
        df_bronze_cleaned = df_bronze.withColumn(
            "salesorderlinenumber", 
            F.when(F.col("salesorderlinenumber").rlike("[^0-9]"), None)  # Remove non-numeric characters
            .otherwise(F.col("salesorderlinenumber"))
        )
        
        df_transformed = df_bronze_cleaned.select(
            F.col("salesordernumber"),
            F.col("salesorderlinenumber"),
            F.col("ordertype"),
            F.col("quantity").cast("int"),
            F.col("taxamount").cast("decimal(7,4)"),
            F.col("unitprice").cast("decimal(8,4)"),
            F.col("customer_id").cast("int"),
            F.col("product_id").cast("int"),
            F.col("createddate"),
            F.col("processeddate"),
            F.col("supplier_id"),
            F.col("warehouse_id"),
            F.col("year"),
            F.col("month"),
            F.col("day")
        )

        # Data quality checks
        df_validated = df_transformed.withColumn(
        "error_description",
            F.when(F.col("quantity").isNull() | (F.col("quantity") < 0), "Invalid quantity")
            .F.when(F.col("taxamount").isNull() | (F.col("taxamount") < 0), "Invalid tax amount")
            .F.when(F.col("unitprice").isNull() | (F.col("unitprice") < 0), "Invalid unit price")
            .F.when(F.col("customer_id").isNull(), "Missing customer ID")
            .F.when(F.col("product_id").isNull(), "Missing product ID")
            .F.when(F.col("supplier_id").isNull(), "Missing supplier ID")
            .F.when(F.col("warehouse_id").isNull(), "Missing warehouse ID")
            .F.when(F.col("createddate").isNull() | F.col("processeddate").isNull(), "Missing date")
            .F.when(F.col("processeddate") < F.col("createddate"), "Invalid date order")
            .otherwise("Valid")
        ).withColumn(
        "error_flag",
            F.when(F.col("error_description") == "Valid", F.lit(0)).otherwise(F.lit(1))
        ) 
        
        # Split valid vs error records
        valid_records_df = df_validated.filter(F.col("error_flag") == 0)
        print(valid_records_df.count())
        error_records_df = df_validated.filter(F.col("error_flag") == 1)
        print(error_records_df.count())
        DEDUPLICATE_KEYS_PRODUCTS = ["createddate", "customer_id", "product_id"]
        valid_records_df = deduplicate_data(valid_records_df, DEDUPLICATE_KEYS_PRODUCTS, "processeddate")
        
        df_target_sale_order = read_iceberg(spark,ICEBERG_SILVER_SALE_ORDERS,True)

        perform_merge(
            spark = spark,
            target_df = df_target_sale_order, 
            source_df = valid_records_df,
            target_view = "target_dim_sale_order",
            source_view = "source_sale_order",
            merge_conditions = "target.salesordernumber = source.salesordernumber AND target.salesorderlinenumber = source.salesorderlinenumber AND target.product_id = source.product_id AND target.customer_id = source.customer_id AND target.createddate = source.createddate",
            update_set = "target.ordertype = source.ordertype, target.quantity = source.quantity, target.taxamount = source.taxamount, target.unitprice = source.unitprice, target.processeddate = source.processeddate, target.supplier_id = source.supplier_id, target.warehouse_id = source.warehouse_id, target.year = source.year, target.month = source.month, target.day = source.day",
            insert_columns = "salesordernumber, salesorderlinenumber, ordertype, quantity, taxamount, unitprice, customer_id, product_id, createddate, processeddate, supplier_id, warehouse_id, year, month, day",
            insert_values = "source.salesordernumber, source.salesorderlinenumber, source.ordertype, source.quantity, source.taxamount, source.unitprice, source.customer_id, source.product_id, source.createddate, source.processeddate, source.supplier_id, source.warehouse_id, source.year, source.month, source.day"
        )

    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
