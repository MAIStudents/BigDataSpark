package com.bigdataspark.service;

import com.bigdataspark.persistence.repository.fact.FactSalesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class FactSalesLoadService {

    private final FactSalesRepository factSalesRepository;

    public void truncateAndInsertFromStaging() {
        log.info("Transactional: truncate fact_sales and insert from staging");
        factSalesRepository.truncateAndInsertFromStaging();
    }
}
