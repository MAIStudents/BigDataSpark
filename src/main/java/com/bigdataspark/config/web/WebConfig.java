package com.bigdataspark.config.web;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebConfig {

    @Bean
    public OpenAPI openApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("BigDataSpark ETL API")
                        .description("REST API для запуска ETL-пайплайна: mock_data → PostgreSQL snowflake → ClickHouse витрины")
                        .version("1.0.0"));
    }
}
