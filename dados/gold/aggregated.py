import boto3
import duckdb
import os
from datetime import datetime
from dotenv import load_dotenv
import tempfile
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ─── 1. CONEXÕES ───────────────────────────────────────────────

# DuckDB
con = duckdb.connect()

# S3 (Garage)
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="garage",
)

bucket_name = "raw-data"


# ─── 2. FUNÇÃO PARA PEGAR ARQUIVO MAIS RECENTE ─────────────────

def get_latest_s3_object(prefix):
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    latest = sorted(objects["Contents"], key=lambda x: x["LastModified"], reverse=True)[0]
    return latest["Key"]


def load_parquet_from_s3(prefix, table_name):
    key = get_latest_s3_object(prefix)
    print(f"Loading {table_name} from {key}")

    obj = s3.get_object(Bucket=bucket_name, Key=key)
    data = obj["Body"].read()

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name

    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_parquet('{tmp_path}')
    """)

    os.remove(tmp_path)


# ─── 3. CARREGAR CAMADA SILVER ─────────────────────────────────

load_parquet_from_s3("orders/silver/dim_products/", "dim_products")
load_parquet_from_s3("orders/silver/fact_orders/", "fact_orders")

print("\n── Silver loaded into DuckDB ──")
print(con.execute("SELECT COUNT(*) FROM dim_products").fetchall())
print(con.execute("SELECT COUNT(*) FROM fact_orders").fetchall())


# ─── 4. CRIAR CAMADA GOLD ──────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE gold_user_metrics AS
    SELECT
        user_id,
        COUNT(*)                    AS total_orders,
        SUM(cart_total)             AS total_spent,
        SUM(cart_discounted_total)  AS total_spent_discounted,
        SUM(cart_total_quantity)    AS total_items,
        AVG(cart_total)             AS avg_ticket
    FROM fact_orders
    GROUP BY user_id
""")

print("\n── Gold preview ──")
print(con.execute("SELECT * FROM gold_user_metrics LIMIT 5").df())


# ─── 5. CONEXÃO POSTGRES ───────────────────────────────────────

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

cur = conn.cursor()


# ─── 6. CRIAR TABELAS NO POSTGRES ──────────────────────────────

cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id INT PRIMARY KEY,
        title TEXT,
        price FLOAT,
        thumbnail TEXT
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_orders (
        cart_id INT PRIMARY KEY,
        user_id INT,
        product_ids INT[],
        cart_total FLOAT,
        cart_discounted_total FLOAT,
        cart_total_products INT,
        cart_total_quantity INT
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS gold_user_metrics (
        user_id INT PRIMARY KEY,
        total_orders INT,
        total_spent FLOAT,
        total_spent_discounted FLOAT,
        total_items INT,
        avg_ticket FLOAT
    );
""")

conn.commit()


# ─── 7. FUNÇÃO DE CARGA ────────────────────────────────────────

def load_table_to_postgres(source_table, target_table):
    df = con.execute(f"SELECT * FROM {source_table}").df()

    if df.empty:
        print(f"{source_table} vazio, pulando...")
        return

    # 🔥 Converter numpy arrays → listas
    df = df.applymap(
        lambda x: x.tolist() if hasattr(x, "tolist") else x
    )

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    query = f"""
        INSERT INTO {target_table} ({','.join(columns)})
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    execute_values(cur, query, values)
    conn.commit()

    print(f"{len(values)} rows loaded into {target_table}")


# ─── 8. LOAD FINAL ─────────────────────────────────────────────

load_table_to_postgres("dim_products", "dim_products")
load_table_to_postgres("fact_orders", "fact_orders")
load_table_to_postgres("gold_user_metrics", "gold_user_metrics")


cur.close()
conn.close()

print("\n🚀 Gold layer carregada no Postgres com sucesso!")