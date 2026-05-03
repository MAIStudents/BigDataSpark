package com.bigdataspark.persistence.repository.merge.postgres;

import com.bigdataspark.config.db.PostgresDatabaseBeans;
import com.bigdataspark.persistence.query.postgres.merge.MergeQuery;
import com.bigdataspark.persistence.repository.merge.MergeRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class PostgresMergeRepositoryImpl implements MergeRepository {

    @Qualifier(PostgresDatabaseBeans.POSTGRES_NAMED_JDBC_TEMPLATE)
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public PostgresMergeRepositoryImpl(
            @Qualifier(PostgresDatabaseBeans.POSTGRES_NAMED_JDBC_TEMPLATE)
            NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void executeMerge(MergeQuery query) {
        log.debug("Executing merge: {}", query.name());
        jdbcTemplate.getJdbcOperations().execute(query.query());
    }
}
