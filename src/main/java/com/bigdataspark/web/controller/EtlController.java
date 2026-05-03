package com.bigdataspark.web.controller;

import com.bigdataspark.dto.JobOkResponse;
import com.bigdataspark.dto.Result;
import com.bigdataspark.dto.RowsLoadedResponse;
import com.bigdataspark.service.CsvToPostgresLoader;
import com.bigdataspark.service.MockToSnowflakeEtlService;
import com.bigdataspark.service.SnowflakeToClickHouseEtlService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import static com.bigdataspark.constants.GlobalConstants.API_V1;

@RestController
@RequestMapping(API_V1)
@RequiredArgsConstructor
@Tag(name = "ETL", description = "Загрузка mock_data и Spark-джобы")
public class EtlController {

    private final CsvToPostgresLoader csvToPostgresLoader;
    private final MockToSnowflakeEtlService mockToSnowflakeEtlService;
    private final SnowflakeToClickHouseEtlService snowflakeToClickHouseEtlService;

    @PostMapping(value = "/upload_mock", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "Загрузка CSV в public.mock_data (multipart, поле file)")
    public Result<RowsLoadedResponse> uploadMock(@RequestPart("file") MultipartFile file) {
        long rows = csvToPostgresLoader.uploadMultipart(file);
        return Result.successResult(new RowsLoadedResponse(rows));
    }

    @PostMapping("/mock_to_snowflake")
    @Operation(summary = "ETL: mock_data → схема snowflake в PostgreSQL")
    public Result<JobOkResponse> mockToSnowflake() {
        mockToSnowflakeEtlService.run();
        return Result.successResult(new JobOkResponse("COMPLETED"));
    }

    @PostMapping("/snowflake_to_clickhouse")
    @Operation(summary = "ETL: snowflake → витрины в ClickHouse")
    public Result<JobOkResponse> snowflakeToClickhouse() {
        snowflakeToClickHouseEtlService.run();
        return Result.successResult(new JobOkResponse("COMPLETED"));
    }
}
