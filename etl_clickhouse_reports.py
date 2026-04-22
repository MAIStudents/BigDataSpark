"""
ETL: Star Schema (PostgreSQL) → 6 Reports (ClickHouse)

Reports:
  1. report_products       – sales by product
  2. report_customers      – sales by customer
  3. report_time           – sales by time
  4. report_stores         – sales by store
  5. report_suppliers      – sales by supplier
  6. report_quality        – product quality / ratings
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://bdlab2_postgres:5432/bdlab2"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:clickhouse://bdlab2_clickhouse:8123/default"
CH_PROPS = {
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

spark = SparkSession.builder \
    .appName("ETL_ClickHouse_Reports") \
    .config("spark.jars.packages",
            "org.postgresql:postgresql:42.6.0,"
            "com.clickhouse:clickhouse-jdbc:0.4.6") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def read_pg(table):
    return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)


def write_ch(df, table):
    df.write.jdbc(CH_URL, table, mode="overwrite", properties=CH_PROPS)
    print(f"{table} written to ClickHouse")


fact      = read_pg("fact_sales")
dim_prod  = read_pg("dim_product")
dim_cust  = read_pg("dim_customer")
dim_store = read_pg("dim_store")
dim_supp  = read_pg("dim_supplier")
dim_date  = read_pg("dim_date")

# Top-10 best-selling products, revenue by category, avg rating & reviews
prod_sales = fact.join(dim_prod, on="product_id") \
    .groupBy("product_id", "product_name", "product_category") \
    .agg(
        F.sum("sale_quantity").alias("total_units_sold"),
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("product_rating").alias("avg_rating"),
        F.avg("product_reviews").alias("avg_reviews")
    )

category_revenue = fact.join(dim_prod, on="product_id") \
    .groupBy("product_category") \
    .agg(F.sum("sale_total_price").alias("category_revenue"))

report_products = prod_sales \
    .join(category_revenue, on="product_category") \
    .withColumn("rank_by_units",
                F.rank().over(Window.orderBy(F.desc("total_units_sold")))) \
    .withColumn("is_top10", F.col("rank_by_units") <= 10)

write_ch(report_products, "report_products")

# Top-10 customers by spend, distribution by country, avg check per customer
cust_sales = fact.join(dim_cust, on="customer_id") \
    .groupBy("customer_id", "customer_first_name", "customer_last_name",
             "customer_country") \
    .agg(
        F.sum("sale_total_price").alias("total_spent"),
        F.count("sale_id").alias("order_count"),
        F.avg("sale_total_price").alias("avg_check")
    )

country_dist = cust_sales.groupBy("customer_country") \
    .agg(F.count("customer_id").alias("customers_in_country"))

report_customers = cust_sales \
    .join(country_dist, on="customer_country") \
    .withColumn("rank_by_spend",
                F.rank().over(Window.orderBy(F.desc("total_spent")))) \
    .withColumn("is_top10", F.col("rank_by_spend") <= 10)

write_ch(report_customers, "report_customers")

# Monthly & yearly trends, revenue comparison, avg order size by month
time_sales = fact.join(dim_date, on="date_id") \
    .groupBy("year", "month") \
    .agg(
        F.sum("sale_total_price").alias("monthly_revenue"),
        F.count("sale_id").alias("order_count"),
        F.avg("sale_total_price").alias("avg_order_size"),
        F.sum("sale_quantity").alias("total_units")
    )

yearly = fact.join(dim_date, on="date_id") \
    .groupBy("year") \
    .agg(F.sum("sale_total_price").alias("yearly_revenue"))

report_time = time_sales.join(yearly, on="year")

write_ch(report_time, "report_time")

# Top-5 stores by revenue, distribution by city/country, avg check per store
store_sales = fact.join(dim_store, on="store_id") \
    .groupBy("store_id", "store_name", "store_city", "store_country") \
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.count("sale_id").alias("order_count"),
        F.avg("sale_total_price").alias("avg_check")
    )

city_dist = store_sales.groupBy("store_city") \
    .agg(F.sum("total_revenue").alias("city_revenue"))

country_dist_store = store_sales.groupBy("store_country") \
    .agg(F.sum("total_revenue").alias("country_revenue"))

report_stores = store_sales \
    .join(city_dist, on="store_city") \
    .join(country_dist_store, on="store_country") \
    .withColumn("rank_by_revenue",
                F.rank().over(Window.orderBy(F.desc("total_revenue")))) \
    .withColumn("is_top5", F.col("rank_by_revenue") <= 5)

write_ch(report_stores, "report_stores")

# Top-5 suppliers by revenue, avg product price, distribution by country
supp_sales = fact.join(dim_supp, on="supplier_id") \
    .join(dim_prod, on="product_id") \
    .groupBy("supplier_id", "supplier_name", "supplier_country") \
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.avg("product_price").alias("avg_product_price"),
        F.count("sale_id").alias("order_count")
    )

supp_country_dist = supp_sales.groupBy("supplier_country") \
    .agg(F.sum("total_revenue").alias("country_revenue"))

report_suppliers = supp_sales \
    .join(supp_country_dist, on="supplier_country") \
    .withColumn("rank_by_revenue",
                F.rank().over(Window.orderBy(F.desc("total_revenue")))) \
    .withColumn("is_top5", F.col("rank_by_revenue") <= 5)

write_ch(report_suppliers, "report_suppliers")

# Highest/lowest rated products, rating vs sales correlation, most reviewed
quality = fact.join(dim_prod, on="product_id") \
    .groupBy("product_id", "product_name", "product_category") \
    .agg(
        F.avg("product_rating").alias("avg_rating"),
        F.avg("product_reviews").alias("avg_reviews"),
        F.sum("sale_quantity").alias("total_units_sold"),
        F.sum("sale_total_price").alias("total_revenue")
    ) \
    .withColumn("rank_by_rating_desc",
                F.rank().over(Window.orderBy(F.desc("avg_rating")))) \
    .withColumn("rank_by_rating_asc",
                F.rank().over(Window.orderBy(F.asc("avg_rating")))) \
    .withColumn("rank_by_reviews",
                F.rank().over(Window.orderBy(F.desc("avg_reviews"))))

write_ch(quality, "report_quality")

print("All 6 ClickHouse reports created successfully.")
spark.stop()
