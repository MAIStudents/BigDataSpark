CREATE TABLE dim_date (
    date_key date PRIMARY KEY,
    day integer NOT NULL,
    month integer NOT NULL,
    month_name text NOT NULL,
    quarter integer NOT NULL,
    year integer NOT NULL,
    day_of_week integer NOT NULL,
    day_name text NOT NULL,
    is_weekend integer NOT NULL
);

CREATE TABLE dim_customer (
    customer_key bigint PRIMARY KEY,
    first_name text NOT NULL,
    last_name text NOT NULL,
    email text NOT NULL UNIQUE,
    country text NOT NULL,
    postal_code text,
    pet_type text NOT NULL,
    pet_name text NOT NULL,
    pet_breed text NOT NULL
);

CREATE TABLE dim_seller (
    seller_key bigint PRIMARY KEY,
    first_name text NOT NULL,
    last_name text NOT NULL,
    email text NOT NULL UNIQUE,
    country text NOT NULL,
    postal_code text
);

CREATE TABLE dim_product (
    product_key bigint PRIMARY KEY,
    name text NOT NULL,
    category text NOT NULL,
    pet_category text NOT NULL,
    brand text NOT NULL,
    color text NOT NULL,
    size text NOT NULL,
    material text NOT NULL,
    description text NOT NULL,
    price numeric(10,2) NOT NULL,
    weight numeric(10,2) NOT NULL,
    rating numeric(3,1) NOT NULL,
    reviews integer NOT NULL,
    release_date date NOT NULL,
    expiry_date date NOT NULL
);

CREATE TABLE dim_store (
    store_key bigint PRIMARY KEY,
    name text NOT NULL,
    location text NOT NULL,
    city text NOT NULL,
    state text,
    country text NOT NULL,
    phone text NOT NULL,
    email text NOT NULL
);

CREATE TABLE dim_supplier (
    supplier_key bigint PRIMARY KEY,
    name text NOT NULL,
    contact text NOT NULL,
    email text NOT NULL,
    phone text NOT NULL,
    address text NOT NULL,
    city text NOT NULL,
    country text NOT NULL
);

CREATE TABLE fact_sales (
    sale_key bigint PRIMARY KEY,
    date_key date NOT NULL REFERENCES dim_date (date_key),
    customer_key bigint NOT NULL REFERENCES dim_customer (customer_key),
    seller_key bigint NOT NULL REFERENCES dim_seller (seller_key),
    product_key bigint NOT NULL REFERENCES dim_product (product_key),
    store_key bigint NOT NULL REFERENCES dim_store (store_key),
    supplier_key bigint NOT NULL REFERENCES dim_supplier (supplier_key),
    quantity integer NOT NULL,
    total_price numeric(12,2) NOT NULL
);
