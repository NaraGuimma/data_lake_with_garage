import requests
import boto3
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# API
url = "https://dummyjson.com/carts"
response = requests.get(url)
data = response.json()

# salvar json local temporariamente
file_name = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

with open(file_name, "w") as f:
    json.dump(data, f)

# conexão com Garage (S3 compatible)
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

bucket_name = "raw-data"

# criar bucket se não existir
try:
    s3.create_bucket(Bucket=bucket_name)
except:
    pass

# upload para o data lake
s3.upload_file(
    file_name,
    bucket_name,
    f"orders/raw/{file_name}"
)

print("Upload successful")