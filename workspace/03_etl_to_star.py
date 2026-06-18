from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Postgres-Staging-To-Star") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/postgres"
pg_properties = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

df_mock = spark.read.jdbc(url=pg_url, table="mock_data", properties=pg_properties)

def add_product_fingerprint(df):
    return df.withColumn(
        "product_fingerprint",
        F.md5(F.concat_ws("||", 
            F.lower(F.trim(F.regexp_replace(F.col("product_name"), r"[^\w\s]", ""))),
            F.coalesce(F.lower(F.trim(F.regexp_replace(F.col("product_brand"), r"[^\w\s]", ""))), F.lit("")),
            F.coalesce(F.lower(F.trim(F.regexp_replace(F.col("product_category"), r"[^\w\s]", ""))), F.lit(""))
        ))
    )

df_mock_enhanced = add_product_fingerprint(df_mock)

def save_dimension(df, select_cols, drop_dup_cols, table_name):
    df_dim_new = df.select(select_cols).dropna(subset=drop_dup_cols).dropDuplicates(drop_dup_cols)
    try:
        df_existing = spark.read.jdbc(url=pg_url, table=table_name, properties=pg_properties)
        df_dim_to_insert = df_dim_new.join(df_existing, on=drop_dup_cols, how="left_anti")
    except:
        df_dim_to_insert = df_dim_new
    df_dim_to_insert.write.jdbc(url=pg_url, table=table_name, mode="append", properties=pg_properties)
    return spark.read.jdbc(url=pg_url, table=table_name, properties=pg_properties)

dim_customer = save_dimension(df_mock_enhanced, 
    ["customer_email", "customer_first_name", "customer_last_name", "customer_age", "customer_country", "customer_postal_code"],
    ["customer_email"], "dim_customer")

dim_seller = save_dimension(df_mock_enhanced, 
    ["seller_email", "seller_first_name", "seller_last_name", "seller_country", "seller_postal_code"],
    ["seller_email"], "dim_seller")

dim_store = save_dimension(df_mock_enhanced, 
    ["store_email", "store_name", "store_location", "store_city", "store_state", "store_country", "store_phone"],
    ["store_name"], "dim_store")

dim_supplier = save_dimension(df_mock_enhanced, 
    ["supplier_email", "supplier_name", "supplier_contact", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"],
    ["supplier_name"], "dim_supplier")

product_cols = [
    "product_fingerprint", "product_name", "product_category", "product_brand", "product_material", 
    "product_color", "product_size", "product_weight", "product_description", 
    "product_rating", "product_reviews", "product_release_date", "product_expiry_date"
]

product_window = Window.partitionBy("product_fingerprint").orderBy(
    F.col("product_reviews").desc(), 
    F.col("product_rating").desc()
)

df_product_prep = df_mock_enhanced \
    .dropna(subset=["product_name"]) \
    .withColumn("row_num", F.row_number().over(product_window)) \
    .filter(F.col("row_num") == 1) \
    .select(product_cols)

try:
    df_existing_product = spark.read.jdbc(url=pg_url, table="dim_product", properties=pg_properties)
    df_product_to_insert = df_product_prep.join(df_existing_product, on="product_fingerprint", how="left_anti")
except:
    df_product_to_insert = df_product_prep

df_product_to_insert.write.jdbc(url=pg_url, table="dim_product", mode="append", properties=pg_properties)
dim_product = spark.read.jdbc(url=pg_url, table="dim_product", properties=pg_properties)

df_date_parsed = df_mock_enhanced.select(F.col("sale_date").alias("date_value")).dropna().dropDuplicates(["date_value"]) \
    .withColumn("year", F.year("date_value")) \
    .withColumn("month", F.month("date_value")) \
    .withColumn("day", F.dayofmonth("date_value")) \
    .withColumn("quarter", F.quarter("date_value")) \
    .withColumn("day_of_week", F.dayofweek("date_value")) \
    .withColumn("is_weekend", F.when(F.dayofweek("date_value").isin(1, 7), True).otherwise(False))

try:
    df_existing_date = spark.read.jdbc(url=pg_url, table="dim_date", properties=pg_properties)
    df_date_to_insert = df_date_parsed.join(df_existing_date, on="date_value", how="left_anti")
except:
    df_date_to_insert = df_date_parsed

df_date_to_insert.write.jdbc(url=pg_url, table="dim_date", mode="append", properties=pg_properties)
dim_date = spark.read.jdbc(url=pg_url, table="dim_date", properties=pg_properties)

df_pet_prep = df_mock_enhanced.select("customer_pet_name", "customer_pet_type", "customer_pet_breed", "pet_category", "customer_email").dropna(subset=["customer_pet_name"])
dim_pet_joined = df_pet_prep.join(dim_customer, "customer_email", "inner") \
    .select(F.col("customer_pet_type").alias("pet_type"), F.col("customer_pet_name").alias("pet_name"), F.col("customer_pet_breed").alias("pet_breed"), "pet_category", "customer_id") \
    .dropDuplicates(["pet_name", "customer_id"])

try:
    df_existing_pet = spark.read.jdbc(url=pg_url, table="dim_pet", properties=pg_properties)
    df_pet_to_insert = dim_pet_joined.join(df_existing_pet, on=["pet_name", "customer_id"], how="left_anti")
except:
    df_pet_to_insert = dim_pet_joined

df_pet_to_insert.write.jdbc(url=pg_url, table="dim_pet", mode="append", properties=pg_properties)
dim_pet = spark.read.jdbc(url=pg_url, table="dim_pet", properties=pg_properties)

dim_date = spark.read.jdbc(url=pg_url, table="dim_date", properties=pg_properties).cache()
dim_customer = spark.read.jdbc(url=pg_url, table="dim_customer", properties=pg_properties).cache()
dim_seller = spark.read.jdbc(url=pg_url, table="dim_seller", properties=pg_properties).cache()
dim_product = spark.read.jdbc(url=pg_url, table="dim_product", properties=pg_properties).cache()
dim_store = spark.read.jdbc(url=pg_url, table="dim_store", properties=pg_properties).cache()
dim_supplier = spark.read.jdbc(url=pg_url, table="dim_supplier", properties=pg_properties).cache()
dim_pet = spark.read.jdbc(url=pg_url, table="dim_pet", properties=pg_properties).cache()

fact_sales = df_mock_enhanced.dropna(subset=["sale_quantity"]) \
    .join(F.broadcast(dim_date), df_mock_enhanced.sale_date == dim_date.date_value, "left") \
    .join(F.broadcast(dim_customer), "customer_email", "left") \
    .join(F.broadcast(dim_seller), "seller_email", "left") \
    .join(F.broadcast(dim_product), "product_fingerprint", "left") \
    .join(F.broadcast(dim_store), "store_name", "left") \
    .join(F.broadcast(dim_supplier), "supplier_name", "left") \
    .join(F.broadcast(dim_pet), (df_mock_enhanced.customer_pet_name == dim_pet.pet_name) & (dim_customer.customer_id == dim_pet.customer_id), "left") \
    .select(
        dim_date.date_id, dim_customer.customer_id, dim_seller.seller_id,
        dim_product.product_id, dim_store.store_id, dim_supplier.supplier_id, dim_pet.pet_id,
        F.col("sale_quantity").alias("quantity"), F.col("product_price"), F.col("sale_total_price").alias("total_price")
    )

fact_sales = fact_sales.repartition(4)
fact_sales.write.jdbc(url=pg_url, table="fact_sales", mode="append", properties=pg_properties)

spark.catalog.clearCache()
spark.stop()