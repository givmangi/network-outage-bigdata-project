import os
import time
import boto3
from botocore.exceptions import ClientError

MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]

BUCKETS = ["bronze", "silver", "gold"]

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )

def wait_for_minio():
    while True:
        try:
            get_s3_client().list_buckets()
            print("MinIO is ready.")
            return
        except Exception:
            print("Waiting for MinIO...")
            time.sleep(3)

def create_bucket(s3, bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"Bucket '{bucket_name}' already exists.")
        else:
            raise

def main():
    wait_for_minio()
    s3 = get_s3_client()
    for bucket in BUCKETS:
        create_bucket(s3, bucket)
    print("All buckets ready.")

if __name__ == "__main__":
    main()