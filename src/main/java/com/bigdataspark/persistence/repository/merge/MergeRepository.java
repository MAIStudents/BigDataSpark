package com.bigdataspark.persistence.repository.merge;

import com.bigdataspark.persistence.query.postgres.merge.MergeQuery;

public interface MergeRepository {

    void executeMerge(MergeQuery query);
}
