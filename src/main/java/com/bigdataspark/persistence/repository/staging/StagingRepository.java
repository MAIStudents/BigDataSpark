package com.bigdataspark.persistence.repository.staging;

public interface StagingRepository {

    void dropStagingTables();

    void createStagingTables();
}
