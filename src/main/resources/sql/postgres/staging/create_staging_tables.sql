CREATE TABLE public.etl_stg_country (
    country_name VARCHAR(100) NOT NULL
);

CREATE TABLE public.etl_stg_city (
    city_name VARCHAR(100) NOT NULL,
    country_name VARCHAR(100) NOT NULL
);

CREATE TABLE public.etl_stg_address (
    postal_code VARCHAR(50),
    city_id INTEGER NOT NULL
);

CREATE TABLE public.etl_stg_supplier (
    supplier_name VARCHAR(200) NOT NULL,
    contact VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address_id INTEGER NOT NULL
);

CREATE TABLE public.etl_stg_product (
    product_name VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    price REAL,
    weight REAL,
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(50),
    material VARCHAR(50),
    description VARCHAR(1024),
    rating REAL,
    reviews INTEGER,
    release_date VARCHAR(50),
    expiry_date VARCHAR(50),
    supplier_id INTEGER NOT NULL
);

CREATE TABLE public.etl_stg_customer (
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    age INTEGER,
    email VARCHAR(50) NOT NULL,
    address_id INTEGER NOT NULL
);

CREATE TABLE public.etl_stg_seller (
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(50) NOT NULL,
    address_id INTEGER NOT NULL
);

CREATE TABLE public.etl_stg_store (
    store_name VARCHAR(50) NOT NULL,
    address_id INTEGER NOT NULL,
    phone VARCHAR(50),
    email VARCHAR(50)
);

CREATE TABLE public.etl_stg_pet (
    pet_name VARCHAR(50) NOT NULL,
    pet_type VARCHAR(50),
    pet_breed VARCHAR(50),
    pet_category VARCHAR(50)
);

CREATE TABLE public.etl_stg_fact (
    customer_id INTEGER NOT NULL,
    pet_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    store_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    sale_date VARCHAR(32) NOT NULL,
    quantity INTEGER NOT NULL,
    total_price REAL NOT NULL
);
