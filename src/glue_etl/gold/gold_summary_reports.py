import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from glue_etl.helpers import get_logger,read_iceberg,perform_merge
from glue_etl.helpers import ICEBERG_SILVER_CUSTOMERS,ICEBERG_SILVER_PRODUCTS,ICEBERG_SILVER_SUPPLIERS,ICEBERG_SILVER_WAREHOUSES,ICEBERG_SILVER_SALE_ORDERS,ICEBERG_GOLD_SALE_SUMMARY_COUNTRY,ICEBERG_GOLD_SALE_SUMMARY_DAILY,ICEBERG_GOLD_SALE_SUMMARY_MONTHLY,ICEBERG_GOLD_SALE_SUMMARY

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
        # Load the data from Glue Catalog into DataFrames
        df_sales_order = read_iceberg(spark,ICEBERG_SILVER_SALE_ORDERS,True)
        df_customer = read_iceberg(spark,ICEBERG_SILVER_CUSTOMERS,True)
        df_product = read_iceberg(spark,ICEBERG_SILVER_PRODUCTS,True)
        df_supplier = read_iceberg(spark,ICEBERG_SILVER_SUPPLIERS,True)
        df_warehouse = read_iceberg(spark,ICEBERG_SILVER_WAREHOUSES,True)

        # Perform the necessary joins and aggregations for summary
        df_sales_summary = (
            df_sales_order
            .join(df_customer, df_sales_order.customer_id == df_customer.customer_id, "left")
            .join(df_product, df_sales_order.product_id == df_product.product_id, "left")
            .join(df_supplier, df_sales_order.supplier_id == df_supplier.supplier_id, "left")
            .join(df_warehouse, df_sales_order.warehouse_id == df_warehouse.warehouse_id, "left")
            .groupBy(
                df_customer.customer_name,
                df_product.product_name,
                df_supplier.supplier_name,
                df_warehouse.warehouse_name,
                F.to_date(df_sales_order.createddate).alias("sale_date")
            )
            .agg(
                F.sum(df_sales_order.quantity).alias("total_quantity_sold"),
                F.sum(df_sales_order.quantity * df_sales_order.unitprice).alias("total_sales_amount"),
                F.sum(df_sales_order.taxamount).alias("total_tax_collected")
            )
            .orderBy(F.desc("total_sales_amount"))
        )

        # Load target table as DataFrame
        df_target_summary = read_iceberg(spark,ICEBERG_GOLD_SALE_SUMMARY,True)

        # Sales Summary Merge
        perform_merge(
            spark = spark,
            target_df = df_target_summary,
            source_df = df_sales_summary,
            target_view = "target_summary",
            source_view = "sales_summary",
            merge_conditions = "tgt.customer_name = src.customer_name AND tgt.product_name = src.product_name AND tgt.supplier_name = src.supplier_name AND tgt.warehouse_name = src.warehouse_name AND tgt.sale_date = src.sale_date",
            update_set = "tgt.total_quantity_sold = src.total_quantity_sold, tgt.total_sales_amount = src.total_sales_amount, tgt.total_tax_collected = src.total_tax_collected"
        )


        # Summary By Country 
        df_summary_by_country = (
            df_sales_order
            .join(df_customer, df_sales_order.customer_id == df_customer.customer_id, "left")
            .groupBy(df_customer.country)
            .agg(
                F.sum(df_sales_order.quantity * df_sales_order.unitprice).alias("total_sales_amount"),
                F.sum(df_sales_order.quantity).alias("total_quantity_sold"),
                F.countDistinct(df_sales_order.salesordernumber).alias("total_orders"),
                F.current_timestamp().alias("last_updated")
            )
        )

        # Load target table as DataFrame ICEBERG_GOLD_SALE_SUMMARY_COUNTRY
        df_target_summary_country = read_iceberg(spark,ICEBERG_GOLD_SALE_SUMMARY_COUNTRY,True)

        perform_merge(
        spark = spark,    
        target_df = df_target_summary_country,
        source_df = df_summary_by_country,
        target_view = "target_summary_country",
        source_view = "sales_summary_country",
        merge_conditions = "tgt.country = src.country",
        update_set = "tgt.total_sales_amount = src.total_sales_amount, tgt.total_quantity_sold = src.total_quantity_sold, tgt.total_orders = src.total_orders, tgt.last_updated = src.last_updated"
        )

        # Summary By Supplier Daily
        df_summary_by_supplier_daily = (
            df_sales_order
            .join(df_supplier, df_sales_order.supplier_id == df_supplier.supplier_id, "left")
            .groupBy(
                F.to_date(df_sales_order.createddate).alias("sales_date"),
                df_supplier.supplier_id,
                df_supplier.supplier_name
            )
            .agg(
                F.sum(df_sales_order.quantity * df_sales_order.unitprice).alias("total_sales_amount"),
                F.sum(df_sales_order.quantity).alias("total_quantity_sold"),
                F.countDistinct(df_sales_order.salesordernumber).alias("total_orders"),
                F.current_timestamp().alias("last_updated")
            )
        )

        # Load target table as DataFrame ICEBERG_GOLD_SALE_SUMMARY_DAILY
        df_target_summary_daily = read_iceberg(spark,ICEBERG_GOLD_SALE_SUMMARY_DAILY,True)
        
        perform_merge(
            spark = spark,
            target_df = df_target_summary_daily,
            source_df = df_summary_by_supplier_daily,
            target_view = "target_summary_daily",
            source_view = "sales_summary_daily",
            merge_conditions = "tgt.sales_date = src.sales_date AND tgt.supplier_id = src.supplier_id",
            update_set = "tgt.total_sales_amount = src.total_sales_amount, tgt.total_quantity_sold = src.total_quantity_sold, tgt.total_orders = src.total_orders, tgt.last_updated = src.last_updated"
        )

        # Summary By Supplier Monthly
        df_summary_by_supplier_monthly = (
            df_sales_order
            .join(df_supplier, df_sales_order.supplier_id == df_supplier.supplier_id, "left")
            .groupBy(
                F.date_format(df_sales_order.createddate, 'yyyy-MM').alias("sales_month"),
                df_supplier.supplier_id,
                df_supplier.supplier_name
            )
            .agg(
                F.sum(df_sales_order.quantity * df_sales_order.unitprice).alias("total_sales_amount"),
                F.sum(df_sales_order.quantity).alias("total_quantity_sold"),
                F.countDistinct(df_sales_order.salesordernumber).alias("total_orders"),
                F.current_timestamp().alias("last_updated")
            )
        )
        
        # Load target table as DataFrame ICEBERG_GOLD_SALE_SUMMARY_MONTHLY
        df_target_summary_monthly = read_iceberg(spark,ICEBERG_GOLD_SALE_SUMMARY_MONTHLY,True)

        perform_merge(
            spark = spark,
            target_df = df_target_summary_monthly,
            source_df = df_summary_by_supplier_monthly,
            target_view = "target_summary_monthly",
            source_view = "sales_summary_monthly",
            merge_conditions = "tgt.sales_month = src.sales_month AND tgt.supplier_id = src.supplier_id",
            update_set = "tgt.total_sales_amount = src.total_sales_amount, tgt.total_quantity_sold = src.total_quantity_sold, tgt.total_orders = src.total_orders, tgt.last_updated = src.last_updated"
        )



    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
