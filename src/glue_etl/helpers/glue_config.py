import os

# You can use environment variables for switching across dev/staging/prod
ENV = os.getenv("ENV", "dev")

CONFIG = {
    "dev": {
        "bronze_bucket": "s3://bronze-bucket",
        "iceberg_catalog": "iceberg_catalog",
        "dynamodb_registry_table": "bronze_file_registry",
        "iceberg_tables": {
            "sales_orders": "iceberg_catalog.bronze.sales_orders",
            "customers": "iceberg_catalog.bronze.customers"
        }
    },
    "prod": {
        "bronze_bucket": "s3://bronze-prod-bucket",
        "iceberg_catalog": "iceberg_catalog",
        "dynamodb_registry_table": "bronze_file_registry_prod",
        "iceberg_tables": {
            "sales_orders": "iceberg_catalog.bronze.sales_orders",
            "customers": "iceberg_catalog.bronze.customers"
        }
    }
}

def get_config():
    return CONFIG[ENV]

def configure_iceberg_spark_session(spark, warehouse_path: str, catalog_name: str = "glue_catalog"):
    """
    Applies Iceberg + AWS Glue + S3 Spark configurations to the provided Spark session.

    Parameters:
    - spark: SparkSession
    - warehouse_path: S3 path where Iceberg tables are stored
    - catalog_name: Name of the Iceberg catalog (default: "glue_catalog")
    """
    spark.conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
