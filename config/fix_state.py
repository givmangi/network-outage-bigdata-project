import boto3

# Using the credentials from your stack trace
s3 = boto3.resource('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password123'
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
