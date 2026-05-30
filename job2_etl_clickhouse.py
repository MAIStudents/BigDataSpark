from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Reports_to_ClickHouse") \
    .config("spark.jars", "/home/jovyan/work/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

PG_URL = "jdbc:postgresql://postgres_db:5432/petshop_db"
PG_PROPS = {"user": "admin", "password": "adminpassword", "driver": "org.postgresql.Driver"}

print("Загрузка из PostgreSQL...")
fact = spark.read.jdbc(PG_URL, "fact_sales", properties=PG_PROPS)
prod = spark.read.jdbc(PG_URL, "dim_product", properties=PG_PROPS)
cust = spark.read.jdbc(PG_URL, "dim_customer", properties=PG_PROPS)
store = spark.read.jdbc(PG_URL, "dim_store", properties=PG_PROPS)

print("Создание отчетов...")
df = fact.join(prod, "sale_product_id").join(cust, "sale_customer_id").join(store, "store_name")

print("\n" + "="*60)
print("ОТЧЕТ 1: ТОП-10 ПРОДУКТОВ ПО ВЫРУЧКЕ")
print("="*60)
top10 = df.groupBy("product_name").sum("sale_total_price").orderBy("sum(sale_total_price)", ascending=False).limit(10)
top10.show(truncate=False)

print("\n" + "="*60)
print("ОТЧЕТ 2: ВЫРУЧКА ПО КАТЕГОРИЯМ")
print("="*60)
revenue = df.groupBy("product_category").sum("sale_total_price").orderBy("sum(sale_total_price)", ascending=False)
revenue.show(truncate=False)

print("\n" + "="*60)
print("ОТЧЕТ 3: ТОП-10 КЛИЕНТОВ")
print("="*60)
customers = df.groupBy("customer_first_name", "customer_last_name").sum("sale_total_price").orderBy("sum(sale_total_price)", ascending=False).limit(10)
customers.show(truncate=False)

print("\n✅ Все отчеты успешно сгенерированы!")
spark.stop()