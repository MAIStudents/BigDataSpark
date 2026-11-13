from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("mock_to_star").getOrCreate()
spark._jvm.java.util.Locale.setDefault(spark._jvm.java.util.Locale.ENGLISH)

pg_url = "jdbc:postgresql://postgres:5432/bigdataspark"
pg_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}


def write_pg(df, table):
    (
        df.write.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", table)
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .option("cascadeTruncate", "true")
        .mode("overwrite")
        .save()
    )


mock = spark.read.jdbc(url=pg_url, table="mock_data", properties=pg_props)

for column in ["customer_postal_code", "seller_postal_code", "store_state"]:
    mock = mock.withColumn(column, F.when(F.col(column) == "", None).otherwise(F.col(column)))

dim_date = mock.select("sale_date").distinct().select(
    F.col("sale_date").alias("date_key"),
    F.dayofmonth("sale_date").alias("day"),
    F.month("sale_date").alias("month"),
    F.date_format("sale_date", "MMMM").alias("month_name"),
    F.quarter("sale_date").alias("quarter"),
    F.year("sale_date").alias("year"),
    F.dayofweek("sale_date").alias("day_of_week"),
    F.date_format("sale_date", "EEEE").alias("day_name"),
    F.when(F.dayofweek("sale_date").isin(1, 7), 1).otherwise(0).alias("is_weekend"),
)

dim_customer = (
    mock.select(
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
        F.col("customer_pet_type").alias("pet_type"),
        F.col("customer_pet_name").alias("pet_name"),
        F.col("customer_pet_breed").alias("pet_breed"),
    )
    .distinct()
    .withColumn("customer_key", F.row_number().over(Window.orderBy("email")).cast("long"))
)

dim_seller = (
    mock.select(
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code"),
    )
    .distinct()
    .withColumn("seller_key", F.row_number().over(Window.orderBy("email")).cast("long"))
)

dim_product = (
    mock.select(
        F.col("product_name").alias("name"),
        F.col("product_category").alias("category"),
        F.col("pet_category"),
        F.col("product_brand").alias("brand"),
        F.col("product_color").alias("color"),
        F.col("product_size").alias("size"),
        F.col("product_material").alias("material"),
        F.col("product_description").alias("description"),
        F.col("product_price").alias("price"),
        F.col("product_weight").alias("weight"),
        F.col("product_rating").alias("rating"),
        F.col("product_reviews").alias("reviews"),
        F.col("product_release_date").alias("release_date"),
        F.col("product_expiry_date").alias("expiry_date"),
    )
    .distinct()
    .withColumn("product_key", F.row_number().over(Window.orderBy("name", "brand")).cast("long"))
)

dim_store = (
    mock.select(
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_phone").alias("phone"),
        F.col("store_email").alias("email"),
    )
    .distinct()
    .withColumn("store_key", F.row_number().over(Window.orderBy("name", "city")).cast("long"))
)

dim_supplier = (
    mock.select(
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.col("supplier_country").alias("country"),
    )
    .distinct()
    .withColumn("supplier_key", F.row_number().over(Window.orderBy("name", "email")).cast("long"))
)

m = mock.alias("m")
c = dim_customer.alias("c")
s = dim_seller.alias("s")
p = dim_product.alias("p")
st = dim_store.alias("st")
sup = dim_supplier.alias("sup")

fact_sales = (
    m.join(
        c,
        (F.col("m.customer_first_name") == F.col("c.first_name"))
        & (F.col("m.customer_last_name") == F.col("c.last_name"))
        & (F.col("m.customer_email") == F.col("c.email"))
        & (F.col("m.customer_country") == F.col("c.country"))
        & (F.col("m.customer_postal_code").eqNullSafe(F.col("c.postal_code")))
        & (F.col("m.customer_pet_type") == F.col("c.pet_type"))
        & (F.col("m.customer_pet_name") == F.col("c.pet_name"))
        & (F.col("m.customer_pet_breed") == F.col("c.pet_breed")),
    )
    .join(
        s,
        (F.col("m.seller_first_name") == F.col("s.first_name"))
        & (F.col("m.seller_last_name") == F.col("s.last_name"))
        & (F.col("m.seller_email") == F.col("s.email"))
        & (F.col("m.seller_country") == F.col("s.country"))
        & (F.col("m.seller_postal_code").eqNullSafe(F.col("s.postal_code"))),
    )
    .join(
        p,
        (F.col("m.product_name") == F.col("p.name"))
        & (F.col("m.product_category") == F.col("p.category"))
        & (F.col("m.pet_category") == F.col("p.pet_category"))
        & (F.col("m.product_brand") == F.col("p.brand"))
        & (F.col("m.product_color") == F.col("p.color"))
        & (F.col("m.product_size") == F.col("p.size"))
        & (F.col("m.product_material") == F.col("p.material"))
        & (F.col("m.product_description") == F.col("p.description"))
        & (F.col("m.product_price") == F.col("p.price"))
        & (F.col("m.product_weight") == F.col("p.weight"))
        & (F.col("m.product_rating") == F.col("p.rating"))
        & (F.col("m.product_reviews") == F.col("p.reviews"))
        & (F.col("m.product_release_date") == F.col("p.release_date"))
        & (F.col("m.product_expiry_date") == F.col("p.expiry_date")),
    )
    .join(
        st,
        (F.col("m.store_name") == F.col("st.name"))
        & (F.col("m.store_location") == F.col("st.location"))
        & (F.col("m.store_city") == F.col("st.city"))
        & (F.col("m.store_state").eqNullSafe(F.col("st.state")))
        & (F.col("m.store_country") == F.col("st.country"))
        & (F.col("m.store_phone") == F.col("st.phone"))
        & (F.col("m.store_email") == F.col("st.email")),
    )
    .join(
        sup,
        (F.col("m.supplier_name") == F.col("sup.name"))
        & (F.col("m.supplier_contact") == F.col("sup.contact"))
        & (F.col("m.supplier_email") == F.col("sup.email"))
        & (F.col("m.supplier_phone") == F.col("sup.phone"))
        & (F.col("m.supplier_address") == F.col("sup.address"))
        & (F.col("m.supplier_city") == F.col("sup.city"))
        & (F.col("m.supplier_country") == F.col("sup.country")),
    )
    .select(
        F.col("m.sale_date").alias("date_key"),
        F.col("c.customer_key"),
        F.col("s.seller_key"),
        F.col("p.product_key"),
        F.col("st.store_key"),
        F.col("sup.supplier_key"),
        F.col("m.sale_quantity").alias("quantity"),
        F.col("m.sale_total_price").alias("total_price"),
    )
    .withColumn("sale_key", F.row_number().over(Window.orderBy("date_key", "customer_key")).cast("long"))
    .select(
        "sale_key",
        "date_key",
        "customer_key",
        "seller_key",
        "product_key",
        "store_key",
        "supplier_key",
        "quantity",
        "total_price",
    )
)

write_pg(dim_date, "dim_date")
write_pg(dim_customer, "dim_customer")
write_pg(dim_seller, "dim_seller")
write_pg(dim_product, "dim_product")
write_pg(dim_store, "dim_store")
write_pg(dim_supplier, "dim_supplier")
write_pg(fact_sales, "fact_sales")

spark.stop()
