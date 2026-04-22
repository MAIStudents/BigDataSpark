"""
ETL: mock_data (PostgreSQL) → Star Schema (PostgreSQL)

Dimensions:
  dim_customer, dim_seller, dim_product, dim_store, dim_supplier, dim_date
Fact:
  fact_sales
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_URL = "jdbc:postgresql://bdlab2_postgres:5432/bdlab2"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("ETL_StarSchema") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

src = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)

dim_customer = src.select(
    F.col("sale_customer_id").alias("customer_id"),
    F.col("customer_first_name"),
    F.col("customer_last_name"),
    F.col("customer_age"),
    F.col("customer_email"),
    F.col("customer_country"),
    F.col("customer_postal_code"),
    F.col("customer_pet_type"),
    F.col("customer_pet_name"),
    F.col("customer_pet_breed")
).dropDuplicates(["customer_id"])

dim_customer.write.jdbc(PG_URL, "dim_customer", mode="overwrite", properties=PG_PROPS)

dim_seller = src.select(
    F.col("sale_seller_id").alias("seller_id"),
    F.col("seller_first_name"),
    F.col("seller_last_name"),
    F.col("seller_email"),
    F.col("seller_country"),
    F.col("seller_postal_code")
).dropDuplicates(["seller_id"])

dim_seller.write.jdbc(PG_URL, "dim_seller", mode="overwrite", properties=PG_PROPS)

dim_product = src.select(
    F.col("sale_product_id").alias("product_id"),
    F.col("product_name"),
    F.col("product_category"),
    F.col("product_price"),
    F.col("product_quantity"),
    F.col("pet_category"),
    F.col("product_weight"),
    F.col("product_color"),
    F.col("product_size"),
    F.col("product_brand"),
    F.col("product_material"),
    F.col("product_description"),
    F.col("product_rating"),
    F.col("product_reviews"),
    F.col("product_release_date"),
    F.col("product_expiry_date")
).dropDuplicates(["product_id"])

dim_product.write.jdbc(PG_URL, "dim_product", mode="overwrite", properties=PG_PROPS)

dim_store = src.select(
    F.col("store_name"),
    F.col("store_location"),
    F.col("store_city"),
    F.col("store_state"),
    F.col("store_country"),
    F.col("store_phone"),
    F.col("store_email")
).dropDuplicates(["store_name"]) \
 .withColumn("store_id", F.monotonically_increasing_id())

dim_store.write.jdbc(PG_URL, "dim_store", mode="overwrite", properties=PG_PROPS)

dim_supplier = src.select(
    F.col("supplier_name"),
    F.col("supplier_contact"),
    F.col("supplier_email"),
    F.col("supplier_phone"),
    F.col("supplier_address"),
    F.col("supplier_city"),
    F.col("supplier_country")
).dropDuplicates(["supplier_name"]) \
 .withColumn("supplier_id", F.monotonically_increasing_id())

dim_supplier.write.jdbc(PG_URL, "dim_supplier", mode="overwrite", properties=PG_PROPS)

dim_date = src.select(
    F.to_date(F.col("sale_date"), "M/d/yyyy").alias("sale_date_parsed")
).dropDuplicates() \
 .withColumn("date_id", F.date_format("sale_date_parsed", "yyyyMMdd").cast("int")) \
 .withColumn("year",  F.year("sale_date_parsed")) \
 .withColumn("month", F.month("sale_date_parsed")) \
 .withColumn("day",   F.dayofmonth("sale_date_parsed")) \
 .withColumn("quarter", F.quarter("sale_date_parsed")) \
 .select("date_id", "sale_date_parsed", "year", "month", "day", "quarter")

dim_date.write.jdbc(PG_URL, "dim_date", mode="overwrite", properties=PG_PROPS)

store_lookup   = dim_store.select("store_id", "store_name")
supplier_lookup = dim_supplier.select("supplier_id", "supplier_name")

fact = src \
    .join(store_lookup,    on="store_name",    how="left") \
    .join(supplier_lookup, on="supplier_name", how="left") \
    .withColumn("date_id", F.date_format(
        F.to_date(F.col("sale_date"), "M/d/yyyy"), "yyyyMMdd").cast("int")) \
    .select(
        F.col("id").alias("sale_id"),
        F.col("sale_customer_id").alias("customer_id"),
        F.col("sale_seller_id").alias("seller_id"),
        F.col("sale_product_id").alias("product_id"),
        F.col("store_id"),
        F.col("supplier_id"),
        F.col("date_id"),
        F.col("sale_quantity"),
        F.col("sale_total_price")
    )

fact.write.jdbc(PG_URL, "fact_sales", mode="overwrite", properties=PG_PROPS)

print("Star schema loaded successfully.")
spark.stop()
