package com.bigdataspark.persistence.query.postgres.merge;

import com.bigdataspark.persistence.query.QueryHolder;

public enum MergeQuery implements QueryHolder {

    MERGE_DIM_COUNTRY("/sql/postgres/merge/merge_dim_country.sql"),
    MERGE_DIM_CITY("/sql/postgres/merge/merge_dim_city.sql"),
    MERGE_DIM_ADDRESS("/sql/postgres/merge/merge_dim_address.sql"),
    MERGE_DIM_SUPPLIER("/sql/postgres/merge/merge_dim_supplier.sql"),
    MERGE_DIM_PRODUCT("/sql/postgres/merge/merge_dim_product.sql"),
    MERGE_DIM_CUSTOMER("/sql/postgres/merge/merge_dim_customer.sql"),
    MERGE_DIM_SELLER("/sql/postgres/merge/merge_dim_seller.sql"),
    MERGE_DIM_STORE("/sql/postgres/merge/merge_dim_store.sql"),
    MERGE_DIM_PET("/sql/postgres/merge/merge_dim_pet.sql");

    private final String query;

    MergeQuery(String path) {
        this.query = getQueryValue(path);
    }

    @Override
    public String query() {
        return query;
    }
}
