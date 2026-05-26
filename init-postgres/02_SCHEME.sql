CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INT,
    customer_email VARCHAR(100) UNIQUE,
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(20)
);

CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(100) UNIQUE,
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(20)
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_fingerprint VARCHAR(32) UNIQUE NOT NULL,
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_color VARCHAR(100),
    product_size VARCHAR(50),
    product_weight DECIMAL(10,2),
    product_description TEXT,
    product_rating DECIMAL(3,1),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(255) UNIQUE NOT NULL,
    store_location VARCHAR(255),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_country VARCHAR(100),
    store_phone VARCHAR(20),
    store_email VARCHAR(100)
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255) UNIQUE NOT NULL,
    supplier_contact VARCHAR(100),
    supplier_email VARCHAR(100),
    supplier_phone VARCHAR(20),
    supplier_address VARCHAR(255),
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE TABLE dim_pet (
    pet_id SERIAL PRIMARY KEY,
    pet_type VARCHAR(100),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100),
    pet_category VARCHAR(100),
    customer_id INT REFERENCES dim_customer(customer_id)
);

CREATE TABLE fact_sales (
    sales_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    customer_id INT REFERENCES dim_customer(customer_id),
    seller_id INT REFERENCES dim_seller(seller_id),
    product_id INT REFERENCES dim_product(product_id),
    store_id INT REFERENCES dim_store(store_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    pet_id INT REFERENCES dim_pet(pet_id),
    quantity INT,
    product_price DECIMAL(10,2),
    total_price DECIMAL(10,2)
);