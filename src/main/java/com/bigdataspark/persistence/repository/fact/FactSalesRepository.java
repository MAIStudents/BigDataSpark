package com.bigdataspark.persistence.repository.fact;

public interface FactSalesRepository {

    void truncateAndInsertFromStaging();
}
