from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import glob

spark = SparkSession.builder \
    .appName("ETL_to_PostgreSQL") \
    .config("spark.jars", "/home/jovyan/work/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

PG_URL = "jdbc:postgresql://postgres_db:5432/petshop_db"
PG_PROPS = {"user": "admin", "password": "adminpassword", "driver": "org.postgresql.Driver"}

print("Чтение CSV...")
all_files = glob.glob("/home/jovyan/work/data/*.csv")
print(f"Найдено файлов: {len(all_files)}")

dfs = []
for f in all_files:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f)
    dfs.append(df)
    print(f"  Загружен: {f.split('/')[-1]}")

raw_df = dfs[0]
for df in dfs[1:]:
    raw_df = raw_df.union(df)

print(f"Всего записей: {raw_df.count()}")

print("Создание измерений...")
dim_product = raw_df.select("sale_product_id", "product_name", "product_category").distinct()
dim_customer = raw_df.select("sale_customer_id", "customer_first_name", "customer_last_name").distinct()
dim_store = raw_df.select("store_name", "store_city").distinct()
dim_date = raw_df.select("sale_date").distinct().withColumn("sale_date", to_date("sale_date", "M/d/yyyy"))

print("Создание фактов...")
fact_sales = raw_df.select("sale_quantity", "sale_total_price", "sale_date", "sale_customer_id", "sale_product_id", "store_name")

print("Сохранение в PostgreSQL...")
dim_product.write.jdbc(PG_URL, "dim_product", mode="overwrite", properties=PG_PROPS)
dim_customer.write.jdbc(PG_URL, "dim_customer", mode="overwrite", properties=PG_PROPS)
dim_store.write.jdbc(PG_URL, "dim_store", mode="overwrite", properties=PG_PROPS)
dim_date.write.jdbc(PG_URL, "dim_date", mode="overwrite", properties=PG_PROPS)
fact_sales.write.jdbc(PG_URL, "fact_sales", mode="overwrite", properties=PG_PROPS)

print(" JOB 1 завершен!")
print(f"  dim_product: {dim_product.count()} записей")
print(f"  dim_customer: {dim_customer.count()} записей")
print(f"  dim_store: {dim_store.count()} записей")
print(f"  fact_sales: {fact_sales.count()} записей")

spark.stop()