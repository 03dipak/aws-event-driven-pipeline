import sys
import time
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from glue_etl.helpers import get_logger
import yaml
import boto3
from urllib.parse import urlparse
from io import BytesIO
import textwrap
from datetime import datetime, timezone
from glue_etl.helpers import BUCKET_NAME
from glue_etl.helpers.glue_config import configure_iceberg_spark_session
# ---------- Parse job arguments ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# ---------- Initialize Spark and Glue Context ----------
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
warehouse_path = f"s3://{BUCKET_NAME}/warehouse/"  # update accordingly
meta_schema_path = f"s3://{BUCKET_NAME}/warehouse/meta_db/schema_migrations/"
# Set Iceberg configs
configure_iceberg_spark_session(spark, warehouse_path)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Initialize Logger
logger = get_logger(args['JOB_NAME'])

# ---------- Migration Table ----------
def migration_table_run():
    MIGRATION_TABLE = "glue_catalog.meta_db.schema_migrations"
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
        filename STRING,
        applied_at TIMESTAMP
    )
    USING iceberg
    LOCATION {meta_schema_path}
    TBLPROPERTIES ('format-version' = '2')
    """)

# ---------- Load all YML files from S3 prefix ----------
def list_yml_files(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".yml") or key.endswith(".yaml"):
                files.append(key)
    return files

# ---------- Load a single YML schema ----------
def load_yaml_schema(s3_client, bucket: str, key: str) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read()
    return yaml.safe_load(BytesIO(content))

# ---------- Generate CREATE TABLE SQL ----------
def generate_create_sql(conf: dict) -> str:
    columns = ",\n  ".join([f"{col['name']} {col['type']}" for col in conf["columns"]])
    partition_clause = f"PARTITIONED BY ({', '.join(conf['partitioned_by'])})" if conf.get("partitioned_by") else ""
    location_clause = f"LOCATION '{conf['location']}'" if conf.get("location") else ""

    sql_query = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{conf['database']}.{conf['table']} (
      {columns}
    )
    USING iceberg
    {location_clause}
    {partition_clause}
    TBLPROPERTIES ('format-version'='{conf.get("format_version", "2")}')
    """
    return textwrap.dedent(sql_query).strip()

# ---------- Generate ALTER TABLE SQL ----------
def generate_alter_sql(conf: dict) -> list:
    table_fqn = f"glue_catalog.{conf['database']}.{conf['table']}"
    alter_statements = []

    for action in conf.get("alterations", []):
        if action["action"] == "add_column":
            alter_statements.append(
                f"ALTER TABLE {table_fqn} ADD COLUMN {action['name']} {action['type']}"
            )
        elif action["action"] == "drop_column":
            alter_statements.append(
                f"ALTER TABLE {table_fqn} DROP COLUMN {action['name']}"
            )
        elif action["action"] == "rename_column":
            alter_statements.append(
                f"ALTER TABLE {table_fqn} RENAME COLUMN {action['old_name']} TO {action['new_name']}"
            )
        elif action["action"] == "update_column_type":
            alter_statements.append(
                f"ALTER TABLE {table_fqn} ALTER COLUMN {action['name']} TYPE {action['new_type']}"
            )
        elif action["action"] == "set_tblproperties":
            properties = action.get("properties", {})
            for k, v in properties.items():
                alter_statements.append(
                    f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ('{k}'='{v}')"
                )
        elif action["action"] == "update_partition":
            # Iceberg requires rewriting the table metadata for partitions
            # This drops and re-adds partition spec
            partition_cols = ", ".join(action["partitioned_by"])
            alter_statements.append(
                f"ALTER TABLE {table_fqn} DROP PARTITION FIELD ALL"
            )
            alter_statements.append(
                f"ALTER TABLE {table_fqn} ADD PARTITION FIELD {partition_cols}"
            )
        elif action["action"] == "update_location":
            alter_statements.append(
                f"ALTER TABLE {table_fqn} SET LOCATION '{action['location']}'"
            )
        else:
            raise ValueError(f"Unsupported alter action: {action['action']}")

    return alter_statements

# ---------- Migration Tracking ----------
def is_migration_applied(filename: str) -> bool:
    df = spark.sql("SELECT filename FROM glue_catalog.meta_db.schema_migrations")
    return df.filter(df.filename == filename).count() > 0

def log_migration(filename: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        INSERT INTO glue_catalog.meta_db.schema_migrations (filename, applied_at)
        VALUES ('{filename}', TIMESTAMP '{ts}')
    """)

# ---------- run migrations ----------
def run_migrations():
    start_time = time.time()
    try:
        logger.info(f"Starting ETL Job {args['JOB_NAME']}")
        migration_table_run()
        # S3 location of YAML files
        schema_base_path = warehouse_path + "schema/"
        parsed = urlparse(schema_base_path)
        bucket = parsed.netloc
        base_prefix = parsed.path.lstrip("/")
        s3 = boto3.client("s3")

        # bronze/, silver/, gold/
        for zone in ["bronze", "silver", "gold"]:
            prefix = f"{base_prefix}{zone}/"
            yml_files = list_yml_files(bucket, prefix)

            logger.info(f"🔍 Zone: {zone} | Found {len(yml_files)} files")

            for key in yml_files:
                filename = key.split("/")[-1]
                if is_migration_applied(filename):
                    logger.info(f"✅ Skipping already applied migration: {filename}")
                    continue

                try:
                    schema = load_yaml_schema(s3, bucket, key)
                    migration_type = schema.get("type", "create")

                    if migration_type == "create":
                        logger.info(f"🚀 Executing create migration: {key}")
                        sql = generate_create_sql(schema)
                        spark.sql(sql)
                        
                    elif migration_type == "alter":
                        alter_statements = generate_alter_sql(schema)
                        logger.info(f"🚀 Executing alter migration: {key}")
                        for stmt in alter_statements:
                            logger.info(f"🛠️ Executing: {stmt}")
                            spark.sql(stmt)
                    else:
                        raise ValueError(f"❌ Unsupported migration type: {migration_type}")

                    
                    log_migration(filename)
                except Exception as e:
                    logger.error(f"❌ Failed migration {key}: {str(e)}")
                    
    except Exception as e:
        logger.error(f"ETL Job {args['JOB_NAME']} Failed: {str(e)}")
        raise e
    finally:
        duration = time.time() - start_time
        logger.info(f"ETL Job {args['JOB_NAME']} Duration: {duration:.2f} seconds")
        job.commit() 
