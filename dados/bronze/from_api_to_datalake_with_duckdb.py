import requests
import boto3
import json
import duckdb
import io
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# API
url = "https://dummyjson.com/carts"
response = requests.get(url)
data = response.json()

# carregar lista de carts diretamente no duckdb
carts = data["carts"]

con = duckdb.connect()
con.execute("CREATE TABLE orders AS SELECT * FROM (SELECT UNNEST(?) AS cart)", [carts])

# exportar para parquet em memória
file_name = f"orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
tmp_path = f"/tmp/{file_name}"

con.execute(f"COPY orders TO '{tmp_path}' (FORMAT PARQUET)")

# conexão com Garage (S3 compatible)
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="garage",
)

bucket_name = "raw-data"

# upload para o data lake
with open(tmp_path, "rb") as f:
    s3.put_object(
        Bucket=bucket_name,
        Key=f"orders/raw/{file_name}",
        Body=f.read()
    )

os.remove(tmp_path)

print(f"Upload successful: orders/raw/{file_name}")
print(con.execute("SELECT COUNT(*) as total_carts FROM orders").fetchone())