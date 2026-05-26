from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Postgres-Staging-To-Star") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/postgres"
pg_properties = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

df_mock = spark.read.jdbc(url=pg_url, table="mock_data", properties=pg_properties)

def save_dimension(df, select_cols, drop_dup_cols, table_name):
    df_dim = df.select(select_cols).dropna(subset=drop_dup_cols).dropDuplicates(drop_dup_cols)
    df_dim.write.jdbc(url=pg_url, table=table_name, mode="append", properties=pg_properties)
    return spark.read.jdbc(url=pg_url, table=table_name, properties=pg_properties)

dim_customer = save_dimension(df_mock, 
    ["customer_email", "customer_first_name", "customer_last_name", "customer_age", "customer_country", "customer_postal_code"],
    ["customer_email"], "dim_customer")

dim_seller = save_dimension(df_mock, 
    ["seller_email", "seller_first_name", "seller_last_name", "seller_country", "seller_postal_code"],
    ["seller_email"], "dim_seller")

dim_product = save_dimension(df_mock, 
    ["product_name", "product_category", "product_brand", "product_material", "product_color", "product_size", "product_weight", "product_description", "product_rating", "product_reviews", "product_release_date", "product_expiry_date"],
    ["product_name"], "dim_product")

dim_store = save_dimension(df_mock, 
    ["store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"],
    ["store_name"], "dim_store")

dim_supplier = save_dimension(df_mock, 
    ["supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"],
    ["supplier_name"], "dim_supplier")

df_date_parsed = df_mock.select(F.col("sale_date").alias("date_value")).dropna().dropDuplicates(["date_value"]) \
    .withColumn("year", F.year("date_value")) \
    .withColumn("month", F.month("date_value")) \
    .withColumn("day", F.dayofmonth("date_value")) \
    .withColumn("quarter", F.quarter("date_value")) \
    .withColumn("day_of_week", F.dayofweek("date_value")) \
    .withColumn("is_weekend", F.when(F.dayofweek("date_value").isin(1, 7), True).otherwise(False))
df_date_parsed.write.jdbc(url=pg_url, table="dim_date", mode="append", properties=pg_properties)
dim_date = spark.read.jdbc(url=pg_url, table="dim_date", properties=pg_properties)

df_pet_prep = df_mock.select("customer_pet_name", "customer_pet_type", "customer_pet_breed", "pet_category", "customer_email").dropna(subset=["customer_pet_name"])
dim_pet_joined = df_pet_prep.join(dim_customer, "customer_email", "inner") \
    .select(F.col("customer_pet_type").alias("pet_type"), F.col("customer_pet_name").alias("pet_name"), F.col("customer_pet_breed").alias("pet_breed"), "pet_category", "customer_id") \
    .dropDuplicates(["pet_name", "customer_id"])
dim_pet_joined.write.jdbc(url=pg_url, table="dim_pet", mode="append", properties=pg_properties)
dim_pet = spark.read.jdbc(url=pg_url, table="dim_pet", properties=pg_properties)

fact_sales = df_mock.dropna(subset=["sale_quantity"]) \
    .join(dim_date, df_mock.sale_date == dim_date.date_value, "left") \
    .join(dim_customer, "customer_email", "left") \
    .join(dim_seller, "seller_email", "left") \
    .join(dim_product, "product_name", "left") \
    .join(dim_store, "store_name", "left") \
    .join(dim_supplier, "supplier_name", "left") \
    .join(dim_pet, (df_mock.customer_pet_name == dim_pet.pet_name) & (dim_customer.customer_id == dim_pet.customer_id), "left") \
    .select(
        dim_date.date_id, dim_customer.customer_id, dim_seller.seller_id,
        dim_product.product_id, dim_store.store_id, dim_supplier.supplier_id, dim_pet.pet_id,
        F.col("sale_quantity").alias("quantity"), F.col("product_price"), F.col("sale_total_price").alias("total_price")
    )

fact_sales.write.jdbc(url=pg_url, table="fact_sales", mode="append", properties=pg_properties)

print("ETL: Таблицы Звезды в PostgreSQL успешно наполнены через Spark!")
spark.stop()