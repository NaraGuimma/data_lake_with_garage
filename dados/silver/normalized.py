import boto3
import duckdb
import json
import io
from datetime import datetime
from dotenv import load_dotenv
import os
import tempfile

load_dotenv()

# conexão com Garage (S3 compatible)
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="garage",
)

bucket_name = "raw-data"

# ─── 1. LER O ARQUIVO RAW DO GARAGE ───────────────────────────────────────────

# listar o arquivo mais recente na pasta raw
objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="orders/raw/")
latest = sorted(objects["Contents"], key=lambda x: x["LastModified"], reverse=True)[0]

print(f"Reading: {latest['Key']}")

raw_obj = s3.get_object(Bucket=bucket_name, Key=latest["Key"])
raw_bytes = raw_obj["Body"].read()

# ─── 2. TRANSFORMAR COM DUCKDB ────────────────────────────────────────────────

con = duckdb.connect()

# salvar bytes do raw em arquivo temporário para o duckdb ler
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_raw:
    tmp_raw.write(raw_bytes)
    tmp_raw_path = tmp_raw.name

con.execute(f"CREATE TABLE raw_orders AS SELECT * FROM read_parquet('{tmp_raw_path}')")
os.remove(tmp_raw_path)

# verificar schema
print(con.execute("DESCRIBE raw_orders").df())

# unpacking the cart struct first
con.execute("""
    CREATE TABLE orders_flat AS
    SELECT
        cart.id             AS id,
        cart.products       AS products,
        cart.total          AS total,
        cart.discountedTotal    AS discountedTotal,
        cart.userId         AS userId,
        cart.totalProducts  AS totalProducts,
        cart.totalQuantity  AS totalQuantity
    FROM raw_orders
""")

# dim_products: um produto único por linha (sem duplicatas entre carrinhos)
con.execute("""
    CREATE TABLE dim_products AS
    SELECT DISTINCT
        product.id            AS product_id,
        product.title         AS title,
        product.price         AS price,
        product.thumbnail     AS thumbnail
    FROM orders_flat,
         UNNEST(products) AS t(product)
    ORDER BY product_id
""")

# fact_orders: uma linha por carrinho com array de product_ids
con.execute("""
    CREATE TABLE fact_orders AS
    SELECT
        id                              AS cart_id,
        userId                          AS user_id,
        LIST(product.id)                AS product_ids,
        total                           AS cart_total,
        discountedTotal                 AS cart_discounted_total,
        totalProducts                   AS cart_total_products,
        totalQuantity                   AS cart_total_quantity
    FROM orders_flat,
         UNNEST(products) AS t(product)
    GROUP BY id, userId, total, discountedTotal, totalProducts, totalQuantity
    ORDER BY cart_id
""")

# preview
print("\n── dim_products ──")
print(con.execute("SELECT * FROM dim_products LIMIT 5").df())

print("\n── fact_orders ──")
print(con.execute("SELECT * FROM fact_orders LIMIT 5").df())

print(f"\nTotal unique products : {con.execute('SELECT COUNT(*) FROM dim_products').fetchone()[0]}")
print(f"Total order lines     : {con.execute('SELECT COUNT(*) FROM fact_orders').fetchone()[0]}")

# ─── 3. SALVAR NA CAMADA SILVER ──────────────────────────────────────────────────────

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

def upload_parquet(con, table_name, s3_key):
    tmp = f"/tmp/{table_name}_{timestamp}.parquet"
    con.execute(f"COPY {table_name} TO '{tmp}' (FORMAT PARQUET)")
    with open(tmp, "rb") as f:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=f.read())
    os.remove(tmp)
    print(f"Uploaded: {s3_key}")

upload_parquet(con, "dim_products", f"orders/silver/dim_products/dim_products_{timestamp}.parquet")
upload_parquet(con, "fact_orders",  f"orders/silver/fact_orders/fact_orders_{timestamp}.parquet")

print("\nSilver layer ready!")