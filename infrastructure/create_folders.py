import boto3
import sys

def ensure_folder(bucket_name, folder_path):
    s3 = boto3.client('s3')
    key = folder_path if folder_path.endswith('/') else folder_path + '/'
    s3.put_object(Bucket=bucket_name, Key=f"{key}.keep", Body="")
    print(f"✅ Ensured folder s3://{bucket_name}/{key}")

if __name__ == "__main__":
    bucket = sys.argv[1]
    folders = sys.argv[2:]

    for folder in folders:
        ensure_folder(bucket, folder)
