import boto3
import sys

def create_glue_database(glue, database_name, location_uri):
    try:
        glue.get_database(Name=database_name)
        print(f"✅ Database '{database_name}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': f'{database_name} database for Glue ETL pipeline',
                'LocationUri': location_uri
            }
        )
        print(f"🎉 Created database: {database_name}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python create_multiple_glue_databases.py <region> <bucket-name> <db1,db2,...>")
        sys.exit(1)

    region = sys.argv[1]
    bucket_name = sys.argv[2]
    db_names = sys.argv[3].split(",")

    glue = boto3.client("glue", region_name=region)

    for db in db_names:
        s3_path = f"s3://{bucket_name}/warehouse/{db}/"
        create_glue_database(glue, db.strip(), s3_path)
