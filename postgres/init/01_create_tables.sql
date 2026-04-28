-- Создание таблицы для исходных данных
CREATE TABLE IF NOT EXISTS mock_data (
    id INTEGER PRIMARY KEY,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INTEGER,
    customer_email VARCHAR(200),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(50),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(200),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(50),
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    product_price DECIMAL(10,2),
    product_quantity INTEGER,
    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price DECIMAL(10,2),
    store_name VARCHAR(200),
    store_location VARCHAR(200),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(200),
    pet_category VARCHAR(100),
    product_weight DECIMAL(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating DECIMAL(3,1),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(200),
    supplier_contact VARCHAR(200),
    supplier_email VARCHAR(200),
    supplier_phone VARCHAR(50),
    supplier_address VARCHAR(200),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

-- Создание таблиц для модели звезда

-- Таблица измерений: Клиенты
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(50),
    pet_type VARCHAR(50),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100)
);

-- Таблица измерений: Продавцы
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(50)
);

-- Таблица измерений: Продукты
CREATE TABLE IF NOT EXISTS dim_products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    category VARCHAR(100),
    price DECIMAL(10,2),
    weight DECIMAL(10,2),
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating DECIMAL(3,1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

-- Таблица измерений: Магазины
CREATE TABLE IF NOT EXISTS dim_stores (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    location VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(200)
);

-- Таблица измерений: Поставщики
CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    contact VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100)
);

-- Таблица измерений: Время
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER
);

-- Таблица фактов: Продажи
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES dim_customers(customer_id),
    seller_id INTEGER REFERENCES dim_sellers(seller_id),
    product_id INTEGER REFERENCES dim_products(product_id),
    store_id INTEGER REFERENCES dim_stores(store_id),
    supplier_id INTEGER REFERENCES dim_suppliers(supplier_id),
    time_id INTEGER REFERENCES dim_time(time_id),
    quantity INTEGER,
    total_price DECIMAL(10,2)
);
