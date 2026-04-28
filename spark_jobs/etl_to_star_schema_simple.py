from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, to_date, monotonically_increasing_id

# Создание Spark сессии
spark = SparkSession.builder \
    .appName("ETL to Star Schema - Simplified") \
    .getOrCreate()

# Параметры подключения к PostgreSQL
jdbc_url = "jdbc:postgresql://postgres:5432/bigdata"
connection_properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

print("Чтение исходных данных из PostgreSQL...")
df = spark.read.jdbc(url=jdbc_url, table="mock_data", properties=connection_properties)
print(f"Загружено {df.count()} записей")

# Создание таблицы измерений: Клиенты
print("\nСоздание dim_customers...")
dim_customers = df.select(
    col("sale_customer_id").alias("customer_id"),
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_age").alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country"),
    col("customer_postal_code").alias("postal_code"),
    col("customer_pet_type").alias("pet_type"),
    col("customer_pet_name").alias("pet_name"),
    col("customer_pet_breed").alias("pet_breed")
).distinct()

dim_customers.write.jdbc(url=jdbc_url, table="dim_customers", mode="overwrite", properties=connection_properties)
print(f"✓ dim_customers: {dim_customers.count()} записей")

# Создание таблицы измерений: Продавцы
print("\nСоздание dim_sellers...")
dim_sellers = df.select(
    col("sale_seller_id").alias("seller_id"),
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country"),
    col("seller_postal_code").alias("postal_code")
).distinct()

dim_sellers.write.jdbc(url=jdbc_url, table="dim_sellers", mode="overwrite", properties=connection_properties)
print(f"✓ dim_sellers: {dim_sellers.count()} записей")

# Создание таблицы измерений: Продукты
print("\nСоздание dim_products...")
dim_products = df.select(
    col("sale_product_id").alias("product_id"),
    col("product_name").alias("name"),
    col("product_category").alias("category"),
    col("product_price").alias("price"),
    col("product_weight").alias("weight"),
    col("product_color").alias("color"),
    col("product_size").alias("size"),
    col("product_brand").alias("brand"),
    col("product_material").alias("material"),
    col("product_description").alias("description"),
    col("product_rating").alias("rating"),
    col("product_reviews").alias("reviews"),
    to_date(col("product_release_date"), "M/d/yyyy").alias("release_date"),
    to_date(col("product_expiry_date"), "M/d/yyyy").alias("expiry_date")
).distinct()

dim_products.write.jdbc(url=jdbc_url, table="dim_products", mode="overwrite", properties=connection_properties)
print(f"✓ dim_products: {dim_products.count()} записей")

# Создание таблицы измерений: Магазины
print("\nСоздание dim_stores...")
dim_stores = df.select(
    col("id").alias("store_id"),
    col("store_name").alias("name"),
    col("store_location").alias("location"),
    col("store_city").alias("city"),
    col("store_state").alias("state"),
    col("store_country").alias("country"),
    col("store_phone").alias("phone"),
    col("store_email").alias("email")
).distinct()

dim_stores.write.jdbc(url=jdbc_url, table="dim_stores", mode="overwrite", properties=connection_properties)
print(f"✓ dim_stores: {dim_stores.count()} записей")

# Создание таблицы измерений: Поставщики
print("\nСоздание dim_suppliers...")
dim_suppliers = df.select(
    col("id").alias("supplier_id"),
    col("supplier_name").alias("name"),
    col("supplier_contact").alias("contact"),
    col("supplier_email").alias("email"),
    col("supplier_phone").alias("phone"),
    col("supplier_address").alias("address"),
    col("supplier_city").alias("city"),
    col("supplier_country").alias("country")
).distinct()

dim_suppliers.write.jdbc(url=jdbc_url, table="dim_suppliers", mode="overwrite", properties=connection_properties)
print(f"✓ dim_suppliers: {dim_suppliers.count()} записей")

# Создание таблицы измерений: Время
print("\nСоздание dim_time...")
dim_time = df.select(
    to_date(col("sale_date"), "M/d/yyyy").alias("date")
).distinct() \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("day", dayofmonth(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("time_id", monotonically_increasing_id())

dim_time.write.jdbc(url=jdbc_url, table="dim_time", mode="overwrite", properties=connection_properties)
print(f"✓ dim_time: {dim_time.count()} записей")

# Создание таблицы фактов: Продажи
print("\nСоздание fact_sales...")
fact_sales = df.select(
    col("id").alias("sale_id"),
    col("sale_customer_id").alias("customer_id"),
    col("sale_seller_id").alias("seller_id"),
    col("sale_product_id").alias("product_id"),
    col("id").alias("store_id"),
    col("id").alias("supplier_id"),
    col("sale_quantity").alias("quantity"),
    col("sale_total_price").alias("total_price")
).withColumn("time_id", monotonically_increasing_id())

fact_sales.write.jdbc(url=jdbc_url, table="fact_sales", mode="overwrite", properties=connection_properties)
print(f"✓ fact_sales: {fact_sales.count()} записей")

print("\n" + "="*50)
print("ETL to Star Schema completed successfully!")
print("="*50)

spark.stop()
