from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

spark = SparkSession.builder \
    .appName("Reports to MongoDB") \
    .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017/bigdata") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/bigdata") \
    .config("spark.jars", "/home/jovyan/work/jars/postgresql-42.7.1.jar,/home/jovyan/work/jars/mongo-spark-connector_2.12-10.2.0.jar") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/bigdata"
pg_props = {"user": "user", "password": "password", "driver": "org.postgresql.Driver"}

fact_sales = spark.read.jdbc(url=pg_url, table="fact_sales", properties=pg_props)
dim_products = spark.read.jdbc(url=pg_url, table="dim_products", properties=pg_props)
dim_customers = spark.read.jdbc(url=pg_url, table="dim_customers", properties=pg_props)
dim_time = spark.read.jdbc(url=pg_url, table="dim_time", properties=pg_props)
dim_stores = spark.read.jdbc(url=pg_url, table="dim_stores", properties=pg_props)
dim_suppliers = spark.read.jdbc(url=pg_url, table="dim_suppliers", properties=pg_props)

# 1. Витрина продаж по продуктам
product_sales = fact_sales.join(dim_products, "product_id") \
    .groupBy("product_id", "name", "category", "rating") \
    .agg(
        sum("quantity").alias("total_quantity"),
        sum("total_price").alias("total_revenue"),
        avg("rating").alias("avg_rating"),
        count("sale_id").alias("sales_count")
    )

product_sales.write \
    .format("mongodb") \
    .option("database", "bigdata") \
    .option("collection", "product_sales_report") \
    .mode("overwrite") \
    .save()

# 2. Витрина продаж по клиентам
customer_sales = fact_sales.join(dim_customers, "customer_id") \
    .groupBy("customer_id", "first_name", "last_name", "country") \
    .agg(
        sum("total_price").alias("total_spent"),
        count("sale_id").alias("purchase_count"),
        avg("total_price").alias("avg_check")
    )

customer_sales.write \
    .format("mongodb") \
    .option("database", "bigdata") \
    .option("collection", "customer_sales_report") \
    .mode("overwrite") \
    .save()

# 3. Витрина продаж по времени
time_sales = fact_sales.join(dim_time, fact_sales.time_id == dim_time.time_id) \
    .groupBy("year", "month", "quarter") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("sale_id").alias("sales_count"),
        avg("total_price").alias("avg_order_size")
    )

time_sales.write \
    .format("mongodb") \
    .option("database", "bigdata") \
    .option("collection", "time_sales_report") \
    .mode("overwrite") \
    .save()

# 4. Витрина продаж по магазинам
store_sales = fact_sales.join(dim_stores, "store_id") \
    .groupBy("store_id", "name", "city", "country") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("sale_id").alias("sales_count"),
        avg("total_price").alias("avg_check")
    )

store_sales.write \
    .format("mongodb") \
    .option("database", "bigdata") \
    .option("collection", "store_sales_report") \
    .mode("overwrite") \
    .save()

# 5. Витрина продаж по поставщикам
supplier_sales = fact_sales.join(dim_suppliers, "supplier_id") \
    .join(dim_products, "product_id") \
    .groupBy("supplier_id", dim_suppliers.name.alias("supplier_name"), dim_suppliers.country.alias("supplier_country")) \
    .agg(
        sum("total_price").alias("total_revenue"),
        avg(dim_products.price).alias("avg_product_price"),
        count("sale_id").alias("sales_count")
    )

supplier_sales.write \
    .format("mongodb") \
    .option("database", "bigdata") \
    .option("collection", "supplier_sales_report") \
    .mode("overwrite") \
    .save()

# 6. Витрина качества продукции
product_quality = fact_sales.join(dim_products, "product_id") \
    .groupBy("product_id", "name", "rating", "reviews") \
    .agg(
        sum("quantity").alias("total_sold"),
        avg("rating").alias("avg_rating")
    )

product_quality.write \
    .format("mongodb") \
    .option("database", "bigdata") \
    .option("collection", "product_quality_report") \
    .mode("overwrite") \
    .save()

print("Reports to MongoDB completed successfully!")
spark.stop()
