import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from constants import METADATA_TABLE,BUCKET_NAME
from functools import reduce
from datetime import datetime, timezone
from pyiceberg.catalog import load_catalog

# ----------  Load new files from registry ----------
def get_new_files(table_name):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(METADATA_TABLE)
    response = table.scan(FilterExpression="status = :val", ExpressionAttributeValues={":val": "NEW"})
    return [item["file_key"] for item in response.get("Items", []) if f"/{table_name}/" in item["file_key"]]

# ---------- Read CSV files ----------
def read_csv(spark: SparkSession,schema,file_key):
    path = f"s3://{BUCKET_NAME}/{file_key}"
    print(f"Reading file: {path}")
    return spark.read.option(header = True,mode = "PERMISSIVE",).schema(schema).csv(path)

# ---------- Basic Validation and Is Not Null ----------
def validate_data(df,required_columns):
    conditions = [F.col(c).isNotNull() for c in required_columns]
    return df.filter(reduce(lambda x, y: x & y, conditions))

# ---------- Deduplication ----------
def deduplicate_data(df, keys, order_column):
    window_spec = Window.partitionBy(*keys).orderBy(F.col(order_column).desc())
    return df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")

# ---------- Write to Iceberg ----------
def write_to_iceberg(df, table):
    df.writeTo(table).using("iceberg").tableProperty("format-version", "2").append()

# ---------- Update Registry ----------
def update_metadata(file_key):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(METADATA_TABLE)
    table.update_item(
        Key={"file_key": file_key},
        UpdateExpression="SET #s = :val, processed_at = :ts",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":val": "PROCESSED",
            ":ts": datetime.datetime.now(timezone.utc).isoformat()
        }
    )
    print(f"Updated metadata: {file_key} → PROCESSED")

# ---------- Read from Iceberg ----------
def read_iceberg(spark: SparkSession,full_table_name,is_timestamp = False):
    # Extract the table name from full_table_name if table_name is not explicitly provided
    table_name = full_table_name.split(".")[-1] if "." in full_table_name else full_table_name.split("/")[-1]
    if is_timestamp == True:
        df = spark.read.format("iceberg").load(full_table_name)
    else:
        # 1. Load the Iceberg catalog (Glue Catalog)
        catalog = load_catalog("glue_catalog")
        # 2. Load the Iceberg table
        schema_table = catalog.load_table(table_name)
        # 3. Get the latest snapshot timestamp (milliseconds)
        latest_snapshot = schema_table.snapshot()
        last_snapshot_timestamp = latest_snapshot.timestamp_ms
        # 4. Read the table and filter out records after the latest snapshot timestamp
        df = spark.read.format("iceberg").load(full_table_name) \
        .filter(F.col("ingestion_timestamp") > F.lit(last_snapshot_timestamp))
    
    return df

# ---------- Updated MERGE ----------
def perform_merge(
    spark: SparkSession, 
    target_df,
    source_df,  
    target_view: str, 
    source_view: str, 
    merge_conditions: str,
    update_condition: str = None,
    update_set: str = None,
    insert_columns: str = "*",
    insert_values: str = None
):
    """
    Perform a dynamic MERGE operation using Spark SQL.

    Parameters:
    - spark: SparkSession
    - target_df: Target DataFrame
    - source_df: Source DataFrame
    - target_view: Temp view name for target
    - source_view: Temp view name for source
    - merge_conditions: SQL ON condition for MERGE
    - update_condition: (Optional) condition after WHEN MATCHED AND ...
    - update_set: (Optional) SET clause for UPDATE
    - insert_columns: (Optional) comma-separated columns for INSERT
    - insert_values: (Optional) comma-separated values for INSERT
    """
    # Register temp views
    target_df.createOrReplaceTempView(target_view)
    source_df.createOrReplaceTempView(source_view)

    # Construct MERGE SQL
    merge_query = f"MERGE INTO {target_view} AS tgt\n"
    merge_query += f"USING {source_view} AS src\n"
    merge_query += f"ON {merge_conditions}\n"

    # Handle the UPDATE part
    if update_condition and update_set:
        merge_query += f"WHEN MATCHED AND ({update_condition}) THEN\n"
        merge_query += f"  UPDATE SET {update_set}\n"
    elif update_set:
        merge_query += "WHEN MATCHED THEN\n"
        merge_query += f"  UPDATE SET {update_set}\n"

    # Handle the INSERT part
    if insert_columns == "*" or not insert_values:
        merge_query += "WHEN NOT MATCHED THEN\n  INSERT *\n"
    else:
        merge_query += f"WHEN NOT MATCHED THEN\n  INSERT ({insert_columns})\n"
        merge_query += f"  VALUES ({insert_values})\n"

    # Execute
    spark.sql(merge_query)
