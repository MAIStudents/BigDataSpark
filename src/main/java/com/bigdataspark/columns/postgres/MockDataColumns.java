package com.bigdataspark.columns.postgres;

import lombok.experimental.UtilityClass;

import java.util.List;

@UtilityClass
public class MockDataColumns {

    public static final String ID = "id";
    public static final String CUSTOMER_FIRST_NAME = "customer_first_name";
    public static final String CUSTOMER_LAST_NAME = "customer_last_name";
    public static final String CUSTOMER_AGE = "customer_age";
    public static final String CUSTOMER_EMAIL = "customer_email";
    public static final String CUSTOMER_COUNTRY = "customer_country";
    public static final String CUSTOMER_POSTAL_CODE = "customer_postal_code";
    public static final String CUSTOMER_PET_TYPE = "customer_pet_type";
    public static final String CUSTOMER_PET_NAME = "customer_pet_name";
    public static final String CUSTOMER_PET_BREED = "customer_pet_breed";
    public static final String SELLER_FIRST_NAME = "seller_first_name";
    public static final String SELLER_LAST_NAME = "seller_last_name";
    public static final String SELLER_EMAIL = "seller_email";
    public static final String SELLER_COUNTRY = "seller_country";
    public static final String SELLER_POSTAL_CODE = "seller_postal_code";
    public static final String PRODUCT_NAME = "product_name";
    public static final String PRODUCT_CATEGORY = "product_category";
    public static final String PRODUCT_PRICE = "product_price";
    public static final String PRODUCT_QUANTITY = "product_quantity";
    public static final String SALE_DATE = "sale_date";
    public static final String SALE_CUSTOMER_ID = "sale_customer_id";
    public static final String SALE_SELLER_ID = "sale_seller_id";
    public static final String SALE_PRODUCT_ID = "sale_product_id";
    public static final String SALE_QUANTITY = "sale_quantity";
    public static final String SALE_TOTAL_PRICE = "sale_total_price";
    public static final String STORE_NAME = "store_name";
    public static final String STORE_LOCATION = "store_location";
    public static final String STORE_CITY = "store_city";
    public static final String STORE_STATE = "store_state";
    public static final String STORE_COUNTRY = "store_country";
    public static final String STORE_PHONE = "store_phone";
    public static final String STORE_EMAIL = "store_email";
    public static final String PET_CATEGORY = "pet_category";
    public static final String PRODUCT_WEIGHT = "product_weight";
    public static final String PRODUCT_COLOR = "product_color";
    public static final String PRODUCT_SIZE = "product_size";
    public static final String PRODUCT_BRAND = "product_brand";
    public static final String PRODUCT_MATERIAL = "product_material";
    public static final String PRODUCT_DESCRIPTION = "product_description";
    public static final String PRODUCT_RATING = "product_rating";
    public static final String PRODUCT_REVIEWS = "product_reviews";
    public static final String PRODUCT_RELEASE_DATE = "product_release_date";
    public static final String PRODUCT_EXPIRY_DATE = "product_expiry_date";
    public static final String SUPPLIER_NAME = "supplier_name";
    public static final String SUPPLIER_CONTACT = "supplier_contact";
    public static final String SUPPLIER_EMAIL = "supplier_email";
    public static final String SUPPLIER_PHONE = "supplier_phone";
    public static final String SUPPLIER_ADDRESS = "supplier_address";
    public static final String SUPPLIER_CITY = "supplier_city";
    public static final String SUPPLIER_COUNTRY = "supplier_country";

    public static final List<String> EXPECTED_HEADER = List.of(
            ID,
            CUSTOMER_FIRST_NAME,
            CUSTOMER_LAST_NAME,
            CUSTOMER_AGE,
            CUSTOMER_EMAIL,
            CUSTOMER_COUNTRY,
            CUSTOMER_POSTAL_CODE,
            CUSTOMER_PET_TYPE,
            CUSTOMER_PET_NAME,
            CUSTOMER_PET_BREED,
            SELLER_FIRST_NAME,
            SELLER_LAST_NAME,
            SELLER_EMAIL,
            SELLER_COUNTRY,
            SELLER_POSTAL_CODE,
            PRODUCT_NAME,
            PRODUCT_CATEGORY,
            PRODUCT_PRICE,
            PRODUCT_QUANTITY,
            SALE_DATE,
            SALE_CUSTOMER_ID,
            SALE_SELLER_ID,
            SALE_PRODUCT_ID,
            SALE_QUANTITY,
            SALE_TOTAL_PRICE,
            STORE_NAME,
            STORE_LOCATION,
            STORE_CITY,
            STORE_STATE,
            STORE_COUNTRY,
            STORE_PHONE,
            STORE_EMAIL,
            PET_CATEGORY,
            PRODUCT_WEIGHT,
            PRODUCT_COLOR,
            PRODUCT_SIZE,
            PRODUCT_BRAND,
            PRODUCT_MATERIAL,
            PRODUCT_DESCRIPTION,
            PRODUCT_RATING,
            PRODUCT_REVIEWS,
            PRODUCT_RELEASE_DATE,
            PRODUCT_EXPIRY_DATE,
            SUPPLIER_NAME,
            SUPPLIER_CONTACT,
            SUPPLIER_EMAIL,
            SUPPLIER_PHONE,
            SUPPLIER_ADDRESS,
            SUPPLIER_CITY,
            SUPPLIER_COUNTRY
    );
}
