import boto3
from dotenv import load_dotenv
import os

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="garage"
)

# listar buckets
buckets = s3.list_buckets()
print("Buckets:", [b["Name"] for b in buckets["Buckets"]])

# listar arquivos dentro do bucket
objects = s3.list_objects_v2(Bucket="raw-data", Prefix="orders/silver/")
for obj in objects.get("Contents", []):
    print(obj["Key"], "-", obj["Size"], "bytes")