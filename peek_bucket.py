import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
    aws_access_key_id=os.environ.get("MINIO_ROOT_USER"),
    aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD"),
    region_name="us-east-1"
)

bucket = os.environ.get("S3_BUCKET_BRONZE", "bronze")
prefix = "ioda/signals/" # Change this to look at events or signals!

print(f"Looking inside s3://{bucket}/{prefix} ...\n")

response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

if "Contents" in response:
    for obj in response["Contents"]:
        print(f"File: {obj['Key']}")
        print(f"Size: {obj['Size']} bytes | Modified: {obj['LastModified']}\n")
else:
    print("Bucket or prefix is empty!")
