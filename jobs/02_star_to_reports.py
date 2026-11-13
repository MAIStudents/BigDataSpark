from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("star_to_reports").getOrCreate()

pg_url = "jdbc:postgresql://postgres:5432/bigdataspark"
pg_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://clickhouse:8123/default"


def read_pg(table):
    return spark.read.jdbc(url=pg_url, table=table, properties=pg_props)


def write_ch(df, table, order_by):
    (
        df.write.format("jdbc")
        .option("url", ch_url)
        .option("dbtable", table)
        .option("user", "default")
        .option("password", "clickhouse")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("createTableOptions", "ENGINE = MergeTree ORDER BY " + order_by)
        .mode("overwrite")
        .save()
    )


fact_sales = read_pg("fact_sales")
dim_date = read_pg("dim_date")
dim_customer = read_pg("dim_customer")
dim_product = read_pg("dim_product").select(
    F.col("product_key"),
    F.col("name").alias("product_name"),
    F.col("category").alias("product_category"),
    F.col("pet_category"),
    F.col("brand").alias("product_brand"),
    F.col("color").alias("product_color"),
    F.col("size").alias("product_size"),
    F.col("material").alias("product_material"),
    F.col("description").alias("product_description"),
    F.col("price").alias("product_price"),
    F.col("weight").alias("product_weight"),
    F.col("rating").alias("product_rating"),
    F.col("reviews").alias("product_reviews"),
    F.col("release_date").alias("product_release_date"),
    F.col("expiry_date").alias("product_expiry_date"),
)
dim_store = read_pg("dim_store")
dim_supplier = read_pg("dim_supplier")

product_sales = (
    fact_sales.join(dim_product, "product_key")
    .groupBy("product_name", "product_category", "product_brand")
    .agg(
        F.count(F.lit(1)).cast("long").alias("sales_cnt"),
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.round(F.avg("product_price"), 2).cast("double").alias("avg_price"),
        F.round(F.avg("product_rating"), 2).cast("double").alias("avg_rating"),
        F.round(F.avg("product_reviews"), 2).cast("double").alias("avg_reviews"),
    )
)

customer_sales = (
    fact_sales.join(dim_customer, "customer_key")
    .groupBy("first_name", "last_name", "email", "country")
    .agg(
        F.count(F.lit(1)).cast("long").alias("sales_cnt"),
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.round(F.avg("total_price"), 2).cast("double").alias("avg_check"),
    )
)

time_sales = (
    fact_sales.join(dim_date, "date_key")
    .groupBy("year", "month", "month_name")
    .agg(
        F.count(F.lit(1)).cast("long").alias("sales_cnt"),
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.round(F.avg("total_price"), 2).cast("double").alias("avg_order_size"),
    )
)

store_sales = (
    fact_sales.join(dim_store, "store_key")
    .groupBy(
        F.col("name").alias("store_name"),
        F.col("city").alias("store_city"),
        F.col("country").alias("store_country"),
    )
    .agg(
        F.count(F.lit(1)).cast("long").alias("sales_cnt"),
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
        F.round(F.avg("total_price"), 2).cast("double").alias("avg_check"),
    )
)

supplier_prices = (
    fact_sales.select("supplier_key", "product_key")
    .distinct()
    .join(dim_product.select("product_key", "product_price"), "product_key")
    .groupBy("supplier_key")
    .agg(F.round(F.avg("product_price"), 2).cast("double").alias("avg_product_price"))
)

supplier_sales = (
    fact_sales.join(dim_supplier, "supplier_key")
    .groupBy("supplier_key", "name", "city", "country")
    .agg(
        F.count(F.lit(1)).cast("long").alias("sales_cnt"),
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
    )
    .join(supplier_prices, "supplier_key")
    .select(
        F.col("name").alias("supplier_name"),
        F.col("city").alias("supplier_city"),
        F.col("country").alias("supplier_country"),
        "sales_cnt",
        "total_revenue",
        "avg_product_price",
    )
)

product_quality = (
    fact_sales.join(dim_product, "product_key")
    .groupBy("product_name", "product_category", "product_brand")
    .agg(
        F.round(F.avg("product_rating"), 2).cast("double").alias("avg_rating"),
        F.round(F.avg("product_reviews"), 2).cast("double").alias("avg_reviews"),
        F.count(F.lit(1)).cast("long").alias("sales_cnt"),
        F.round(F.sum("total_price"), 2).cast("double").alias("total_revenue"),
    )
)

write_ch(product_sales, "reports.rpt_product_sales", "(product_name, product_brand)")
write_ch(customer_sales, "reports.rpt_customer_sales", "email")
write_ch(time_sales, "reports.rpt_time_sales", "(year, month)")
write_ch(store_sales, "reports.rpt_store_sales", "(store_country, store_city, store_name)")
write_ch(supplier_sales, "reports.rpt_supplier_sales", "(supplier_country, supplier_name)")
write_ch(product_quality, "reports.rpt_product_quality", "(product_name, product_brand)")

spark.stop()
