package com.bigdataspark.service;

import com.bigdataspark.persistence.repository.schema.ClickHouseSchemaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ClickHouseSchemaService {

    private final ClickHouseSchemaRepository clickHouseSchemaRepository;

    public void recreateMartTables() {
        clickHouseSchemaRepository.recreateMartTables();
    }
}
