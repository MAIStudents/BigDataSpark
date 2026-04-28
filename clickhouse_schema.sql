-- Создание таблиц в ClickHouse

-- Таблица отчета по продуктам
CREATE TABLE IF NOT EXISTS product_sales_report (
    product_id Int32,
    name String,
    category String,
    rating Decimal(3,1),
    total_quantity Int64,
    total_revenue Decimal(10,2),
    avg_rating Decimal(3,1),
    sales_count Int64
) ENGINE = MergeTree()
ORDER BY product_id;

-- Таблица отчета по клиентам
CREATE TABLE IF NOT EXISTS customer_sales_report (
    customer_id Int32,
    first_name String,
    last_name String,
    country String,
    total_spent Decimal(10,2),
    purchase_count Int64,
    avg_check Decimal(10,2)
) ENGINE = MergeTree()
ORDER BY customer_id;

-- Таблица отчета по времени
CREATE TABLE IF NOT EXISTS time_sales_report (
    year Int32,
    month Int32,
    quarter Int32,
    total_revenue Decimal(10,2),
    sales_count Int64,
    avg_order_size Decimal(10,2)
) ENGINE = MergeTree()
ORDER BY (year, month);

-- Таблица отчета по магазинам
CREATE TABLE IF NOT EXISTS store_sales_report (
    store_id Int32,
    name String,
    city String,
    country String,
    total_revenue Decimal(10,2),
    sales_count Int64,
    avg_check Decimal(10,2)
) ENGINE = MergeTree()
ORDER BY store_id;

-- Таблица отчета по поставщикам
CREATE TABLE IF NOT EXISTS supplier_sales_report (
    supplier_id Int32,
    supplier_name String,
    supplier_country String,
    total_revenue Decimal(10,2),
    avg_product_price Decimal(10,2),
    sales_count Int64
) ENGINE = MergeTree()
ORDER BY supplier_id;

-- Таблица отчета по качеству продукции
CREATE TABLE IF NOT EXISTS product_quality_report (
    product_id Int32,
    name String,
    rating Decimal(3,1),
    reviews Int32,
    total_sold Int64,
    avg_rating Decimal(3,1)
) ENGINE = MergeTree()
ORDER BY product_id;
