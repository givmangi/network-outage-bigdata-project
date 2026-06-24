"""
config/fix_state.py
===================
Pipeline role:  Maintenance / disaster recovery utility (run manually as needed).

Trigger:        Run this script as step 4 of the silver batch sequence (see
                README §5.9) whenever silver batch jobs have been run, since
                overwriting silver-layer partitions corrupts the Spark
                _spark_metadata transaction logs that spark-silver-stream depends
                on. Always follow the full sequence in §5.9:
                  1. Stop spark-silver-stream and spark-gold-stream
                  2. Run spark-silver-ioda and spark-silver-ripe batch jobs
                  3. Run spark-gold batch job
                  4. Run this script  <-- you are here
                  5. Restart spark-silver-stream and spark-gold-stream

Usage:
    python3 config/fix_state.py

Inputs:         MinIO `silver` bucket — checkpoint directories under
                `_checkpoints/` and Spark metadata logs under
                `{ioda,ripe}/.../_spark_metadata/`

Outputs:        Deletes all streaming checkpoint and metadata state from the
                `silver` bucket so that spark-silver-stream can restart cleanly.

Credentials:    Reads MINIO_ROOT_USER and MINIO_ROOT_PASSWORD from .env via
                python-dotenv. MinIO endpoint defaults to http://localhost:9000
                but can be overridden with MINIO_ENDPOINT in .env.
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()

endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
access_key = os.environ["MINIO_ROOT_USER"]
secret_key = os.environ["MINIO_ROOT_PASSWORD"]

s3 = boto3.resource(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)
bucket = s3.Bucket('silver')

print("Purging corrupted Spark streaming state...")

# 1. Delete all streaming checkpoints
for obj in bucket.objects.filter(Prefix='_checkpoints/'):
    print(f"Deleting {obj.key}")
    obj.delete()

# 2. Delete the Spark metadata transaction logs in the silver tables
for prefix in ['ioda/signals/', 'ioda/alerts/', 'ioda/events/', 'ripe/ping/']:
    metadata_path = f"{prefix}_spark_metadata/"
    for obj in bucket.objects.filter(Prefix=metadata_path):
        print(f"Deleting {obj.key}")
        obj.delete()

print("Cleanup complete! You can now restart your streams.")
