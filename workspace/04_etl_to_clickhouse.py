from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("Postgres-To-ClickHouse-DataMarts") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.6.0-patch5") \
    .getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/postgres"
pg_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://clickhouse:8123/reports"
ch_props = {
    "user": "pyspark_user",
    "password": "pyspark_password",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}


def write_to_clickhouse(df, table_name):
    
    df.write \
      .option("createTableOptions", "ENGINE = Log") \
      .jdbc(url=ch_url, table=table_name, mode="overwrite", properties=ch_props)


fact_sales = spark.read.jdbc(url=pg_url, table="fact_sales", properties=pg_props)
dim_product = spark.read.jdbc(url=pg_url, table="dim_product", properties=pg_props)
dim_customer = spark.read.jdbc(url=pg_url, table="dim_customer", properties=pg_props)
dim_store = spark.read.jdbc(url=pg_url, table="dim_store", properties=pg_props)
dim_supplier = spark.read.jdbc(url=pg_url, table="dim_supplier", properties=pg_props)
dim_date = spark.read.jdbc(url=pg_url, table="dim_date", properties=pg_props)


report_product_sales = fact_sales.join(dim_product, "product_id") \
    .groupBy("product_name", "product_category") \
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("total_price").alias("total_revenue"),
        F.max("product_rating").alias("product_rating"),
        F.max("product_reviews").alias("product_reviews")
    )
write_to_clickhouse(report_product_sales, "report_product_sales")


report_customer_sales = fact_sales.join(dim_customer, "customer_id") \
    .groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country") \
    .agg(
        F.sum("total_price").alias("total_spent"),
        F.count("sales_id").alias("orders_count"),
        F.round(F.avg("total_price"), 2).alias("average_check")
    )
write_to_clickhouse(report_customer_sales, "report_customer_sales")


report_time_sales = fact_sales.join(dim_date, "date_id") \
    .groupBy("year", "month") \
    .agg(
        F.count("sales_id").alias("sales_count"),
        F.sum("total_price").alias("total_revenue"),
        F.round(F.avg("quantity"), 2).alias("avg_order_size")
    )
write_to_clickhouse(report_time_sales, "report_time_sales")


report_store_sales = fact_sales.join(dim_store, "store_id") \
    .groupBy("store_name", "store_city", "store_country") \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.count("sales_id").alias("sales_count"),
        F.round(F.avg("total_price"), 2).alias("average_check")
    )
write_to_clickhouse(report_store_sales, "report_store_sales")


report_supplier_sales = fact_sales.join(dim_supplier, "supplier_id") \
    .groupBy("supplier_name", "supplier_country") \
    .agg(
        F.sum("total_price").alias("total_revenue"),
        F.round(F.avg("product_price"), 2).alias("avg_product_price")
    )
write_to_clickhouse(report_supplier_sales, "report_supplier_sales")


report_product_quality = fact_sales.join(dim_product, "product_id") \
    .groupBy("product_name") \
    .agg(
        F.max("product_rating").alias("rating"),
        F.max("product_reviews").alias("reviews_count"),
        F.sum("quantity").alias("total_units_sold")
    )
write_to_clickhouse(report_product_quality, "report_product_quality")

print("ClickHouse: Все 6 аналитических витрин успешно созданы!")
spark.stop()