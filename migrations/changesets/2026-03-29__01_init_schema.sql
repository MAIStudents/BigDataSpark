-- liquibase formatted sql
-- changeset admin:0

CREATE SCHEMA snowflake;

-- Таблица стран
CREATE TABLE snowflake.dim_country
(
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE
);

-- Таблица городов
CREATE TABLE snowflake.dim_city
(
    city_id    SERIAL PRIMARY KEY,
    city_name  VARCHAR(50),
    country_id INTEGER REFERENCES snowflake.dim_country (country_id),
    UNIQUE (city_name, country_id)
);

-- Таблица адресов
CREATE TABLE snowflake.dim_address
(
    address_id  SERIAL PRIMARY KEY,
    postal_code VARCHAR(50),
    city_id     INTEGER REFERENCES snowflake.dim_city (city_id)
);

-- Таблица клиентов
CREATE TABLE snowflake.dim_customer
(
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    age         INTEGER,
    email       VARCHAR(50),
    address_id  INTEGER REFERENCES snowflake.dim_address (address_id)
);

-- Таблица питомцев
CREATE TABLE snowflake.dim_pet
(
    pet_id       SERIAL PRIMARY KEY,
    pet_name     VARCHAR(50),
    pet_type     VARCHAR(50),
    pet_breed    VARCHAR(50),
    pet_category VARCHAR(50)
);

-- Таблица продавцов
CREATE TABLE snowflake.dim_seller
(
    seller_id  SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name  VARCHAR(50),
    email      VARCHAR(50),
    address_id INTEGER REFERENCES snowflake.dim_address (address_id)
);

-- Таблица магазинов
CREATE TABLE snowflake.dim_store
(
    store_id   SERIAL PRIMARY KEY,
    store_name VARCHAR(50),
    address_id INTEGER REFERENCES snowflake.dim_address (address_id),
    phone      VARCHAR(50),
    email      VARCHAR(50)
);

-- Таблица поставщиков
CREATE TABLE snowflake.dim_supplier
(
    supplier_id   SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200),
    contact       VARCHAR(100),
    email         VARCHAR(255),
    phone         VARCHAR(50),
    address_id    INTEGER REFERENCES snowflake.dim_address (address_id)
);

-- Таблица продуктов
CREATE TABLE snowflake.dim_product
(
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    category     VARCHAR(50),
    price        REAL,
    weight       REAL,
    color        VARCHAR(50),
    size         VARCHAR(50),
    brand        VARCHAR(50),
    material     VARCHAR(50),
    description  VARCHAR(1024),
    rating       REAL,
    reviews      INTEGER,
    release_date DATE,
    expiry_date  DATE,
    supplier_id  INTEGER REFERENCES snowflake.dim_supplier (supplier_id)
);

-- Таблица фактов продаж
CREATE TABLE snowflake.fact_sales
(
    sale_id     SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES snowflake.dim_customer (customer_id),
    pet_id      INTEGER REFERENCES snowflake.dim_pet (pet_id),
    seller_id   INTEGER REFERENCES snowflake.dim_seller (seller_id),
    store_id    INTEGER REFERENCES snowflake.dim_store (store_id),
    product_id  INTEGER REFERENCES snowflake.dim_product (product_id),
    sale_date   DATE,
    quantity    INTEGER,
    total_price REAL
);

--rollback drop schema snowflake cascade;