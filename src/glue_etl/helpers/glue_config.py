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
