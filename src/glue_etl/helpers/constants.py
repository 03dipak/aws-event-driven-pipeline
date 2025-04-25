from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DecimalType


BUCKET_NAME = "glue-etl-dev-bucket-april2025"
METADATA_TABLE = "bronze_file_registry"

#SALE_ORDER
ICEBERG_TABLE_SALE_ORDERS = "glue_catalog.bronze.sale_orders"
PARTITION_COLUMN_SALE_ORDERS = "createddate"
DEDUPLICATE_KEYS_SALE_ORDERS = ["salesordernumber", "salesorderlinenumber"]
TABLE_NAME_SALE_ORDERS = "sale_orders"
REQUIRED_COLUMNS_SALE_ORDERS = ["salesordernumber", "salesorderlinenumber", "createddate"] #not values
ICEBERG_SILVER_SALE_ORDERS = "glue_catalog.silver.fact_sale_orders"

#CUSTOMERS
ICEBERG_TABLE_CUSTOMERS = "glue_catalog.bronze.customers"
PARTITION_COLUMN_PRODUCTS = "CountryRegion","PostalCode"
DEDUPLICATE_KEYS_CUSTOMERS = ["CustomerID"]
DEDUPLICATE_ORDERBY_CUSTOMERS = "CustomerID"
TABLE_NAME_CUSTOMERS = "customers"
REQUIRED_COLUMNS_CUSTOMERS = ["FirstName", "LastName", "CustomerID", "EmailAddress"] #not values
ICEBERG_SILVER_CUSTOMERS = "glue_catalog.silver.dim_customers"

#PRODUCTS
ICEBERG_TABLE_PRODUCTS = "glue_catalog.bronze.products"
PARTITION_COLUMN_PRODUCTS = "ProductCategoryID"
DEDUPLICATE_KEYS_PRODUCTS = ["ProductID","ProductNumber"]
DEDUPLICATE_ORDERBY_PRODUCTS = "ProductID"
TABLE_NAME_PRODUCTS = "products"
REQUIRED_COLUMNS_PRODUCTS = ["ProductID","ProductNumber"] #not values
ICEBERG_SILVER_PRODUCTS = "glue_catalog.silver.dim_products"

#SUPPLIERS
ICEBERG_TABLE_SUPPLIERS = "glue_catalog.bronze.suppliers"
PARTITION_COLUMN_SUPPLIERS = "country", "state", "postal_code"
DEDUPLICATE_KEYS_SUPPLIERS = ["supplier_id","supplier_name"]
DEDUPLICATE_ORDERBY_SUPPLIERS = "created_at"
TABLE_NAME_SUPPLIERS = "suppliers"
REQUIRED_COLUMNS_SUPPLIERS = ["supplier_id", "supplier_name", "email", "phone"] #not values
ICEBERG_SILVER_SUPPLIERS = "glue_catalog.silver.dim_suppliers"

#WAREHOUSES
ICEBERG_TABLE_WAREHOUSES = "glue_catalog.bronze.warehouses"
PARTITION_COLUMN_WAREHOUSES = "country", "state","postal_code"
DEDUPLICATE_KEYS_WAREHOUSES = ["CustomerID"]
DEDUPLICATE_ORDERBY_WAREHOUSES = "created_at"
TABLE_NAME_WAREHOUSES = "warehouses"
REQUIRED_COLUMNS_WAREHOUSES = ["warehouse_id", "warehouse_name", "country", "location"] #not values
ICEBERG_SILVER_WAREHOUSES = "glue_catalog.silver.dim_warehouses"

ICEBERG_GOLD_SALE_SUMMARY = "glue_catalog.gold.fact_sale_summary"
ICEBERG_GOLD_SALE_SUMMARY_COUNTRY = "glue_catalog.gold.fact_sale_summary_by_country"
ICEBERG_GOLD_SALE_SUMMARY_DAILY = "glue_catalog.gold.fact_sale_summary_by_supplier_daily"
ICEBERG_GOLD_SALE_SUMMARY_MONTHLY = "glue_catalog.gold.fact_sale_summary_by_supplier_monthly"

# ---------- DEFINE PREDEFINED SCHEMA----------
#SALE_ORDERS
SALE_ORDERS_SCHEMA = StructType([
    StructField("salesordernumber", StringType(), True),        # SO55795, SO40860, ...
    StructField("salesorderlinenumber", IntegerType(), True),   # 2, 1, 3, ...
    StructField("createddate", TimestampType(), True),          # 2024-09-21, 2024-11-04, ...
    StructField("processeddate", TimestampType(), True),        # 2024-09-24, 2024-11-07, ...
    StructField("ordertype", StringType(), True),               # M
    StructField("quantity", IntegerType(), True),               # 1, 3, 4, ...
    StructField("unitprice", DoubleType(), True),               # 354.9247384731111, ...
    StructField("taxamount", DoubleType(), True),               # 28.39397907784889, ...
    StructField("customer_id", StringType(), True),             # 30055, 29851, ...
    StructField("product_id", StringType(), True),              # 867, 766, 967, ...
    StructField("supplier_id", StringType(), True),             # SUP0091, SUP0066, ...
    StructField("warehouse_id", StringType(), True)             # WH018, WH029, ...
])
#CUSTOMERS
CUSTOMERS_SCHEMA = StructType([
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("CompanyName", StringType(), True),
    StructField("EmailAddress", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("AddressLine1", StringType(), True),
    StructField("City", StringType(), True),
    StructField("CountryRegion", StringType(), True),
    StructField("PostalCode", StringType(), True)
])

#PRODUCTS
PRODUCTS_SCHEMA = StructType([
    StructField("ProductID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("ProductNumber", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("StandardCost", DecimalType(10, 2), True),
    StructField("ListPrice", DecimalType(10, 2), True),
    StructField("Size", StringType(), True),
    StructField("Weight", DecimalType(10, 2), True),
    StructField("ProductCategoryID", IntegerType(), True),
    StructField("ProductModelID", IntegerType(), True),
    StructField("SellStartDate", TimestampType(), True),
    StructField("ThumbNailPhoto", StringType(), True),
    StructField("ThumbnailPhotoFileName", StringType(), True),
    StructField("rowguid", StringType(), True)
])

#SUPPLIERS
SUPPLIERS_SCHEMA = StructType([
    StructField("supplier_id", StringType(), True),
    StructField("supplier_name", StringType(), True),
    StructField("contact_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("business_category", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])

#WAREHOUSES
WAREHOUSES_SCHEMA = StructType([
    StructField("warehouse_id", StringType(), True),
    StructField("warehouse_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("capacity", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])