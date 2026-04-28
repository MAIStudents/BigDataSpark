-- Создание keyspace
CREATE KEYSPACE IF NOT EXISTS bigdata 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE bigdata;

-- Таблица отчета по продуктам
CREATE TABLE IF NOT EXISTS product_sales_report (
    product_id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    rating DECIMAL,
    total_quantity BIGINT,
    total_revenue DECIMAL,
    avg_rating DECIMAL,
    sales_count BIGINT
);

-- Таблица отчета по клиентам
CREATE TABLE IF NOT EXISTS customer_sales_report (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    country TEXT,
    total_spent DECIMAL,
    purchase_count BIGINT,
    avg_check DECIMAL
);

-- Таблица отчета по времени
CREATE TABLE IF NOT EXISTS time_sales_report (
    year INT,
    month INT,
    quarter INT,
    total_revenue DECIMAL,
    sales_count BIGINT,
    avg_order_size DECIMAL,
    PRIMARY KEY (year, month)
);

-- Таблица отчета по магазинам
CREATE TABLE IF NOT EXISTS store_sales_report (
    store_id INT PRIMARY KEY,
    name TEXT,
    city TEXT,
    country TEXT,
    total_revenue DECIMAL,
    sales_count BIGINT,
    avg_check DECIMAL
);

-- Таблица отчета по поставщикам
CREATE TABLE IF NOT EXISTS supplier_sales_report (
    supplier_id INT PRIMARY KEY,
    supplier_name TEXT,
    supplier_country TEXT,
    total_revenue DECIMAL,
    avg_product_price DECIMAL,
    sales_count BIGINT
);

-- Таблица отчета по качеству продукции
CREATE TABLE IF NOT EXISTS product_quality_report (
    product_id INT PRIMARY KEY,
    name TEXT,
    rating DECIMAL,
    reviews INT,
    total_sold BIGINT,
    avg_rating DECIMAL
);
