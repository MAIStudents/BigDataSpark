package com.bigdataspark.service;

import com.bigdataspark.columns.postgres.MockDataColumns;
import com.bigdataspark.columns.postgres.PublicTables;
import com.bigdataspark.config.db.PostgresConnectionProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

@Service
@RequiredArgsConstructor
@Slf4j
public class CsvToPostgresLoader {

    private final SparkSession sparkSession;
    private final PostgresConnectionProperties postgresConnectionProperties;

    public long uploadMultipart(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("File is empty");
        }
        try {
            Path temp = Files.createTempFile("mock-upload-", ".csv");
            try {
                file.transferTo(temp);
                return loadCsv(temp);
            } finally {
                Files.deleteIfExists(temp);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to store upload", e);
        }
    }

    public long loadCsv(Path csvPath) {
        if (csvPath == null || !csvPath.toFile().isFile()) {
            throw new IllegalArgumentException("CSV file not found: " + csvPath);
        }
        Dataset<Row> df = sparkSession.read()
                .option("header", true)
                .option("multiLine", true)
                .option("encoding", "UTF-8")
                .csv(csvPath.toAbsolutePath().toString());
        validateHeader(Arrays.asList(df.columns()));
        Dataset<Row> typed = castToMockDataSchema(df);
        long count = typed.count();
        typed.write()
                .mode(SaveMode.Append)
                .jdbc(
                        postgresConnectionProperties.url(),
                        PublicTables.MOCK_DATA,
                        postgresConnectionProperties.sparkConnectionProperties()
                );
        log.info("Uploaded {} rows to {}", count, PublicTables.MOCK_DATA);
        return count;
    }

    private static Dataset<Row> castToMockDataSchema(Dataset<Row> df) {
        return df
                .withColumn(MockDataColumns.ID, col(MockDataColumns.ID).cast("long"))
                .withColumn(MockDataColumns.CUSTOMER_AGE, col(MockDataColumns.CUSTOMER_AGE).cast("int"))
                .withColumn(MockDataColumns.PRODUCT_PRICE, col(MockDataColumns.PRODUCT_PRICE).cast("double"))
                .withColumn(MockDataColumns.PRODUCT_QUANTITY, col(MockDataColumns.PRODUCT_QUANTITY).cast("int"))
                .withColumn(MockDataColumns.SALE_CUSTOMER_ID, col(MockDataColumns.SALE_CUSTOMER_ID).cast("int"))
                .withColumn(MockDataColumns.SALE_SELLER_ID, col(MockDataColumns.SALE_SELLER_ID).cast("int"))
                .withColumn(MockDataColumns.SALE_PRODUCT_ID, col(MockDataColumns.SALE_PRODUCT_ID).cast("int"))
                .withColumn(MockDataColumns.SALE_QUANTITY, col(MockDataColumns.SALE_QUANTITY).cast("int"))
                .withColumn(MockDataColumns.SALE_TOTAL_PRICE, col(MockDataColumns.SALE_TOTAL_PRICE).cast("double"))
                .withColumn(MockDataColumns.PRODUCT_WEIGHT, col(MockDataColumns.PRODUCT_WEIGHT).cast("double"))
                .withColumn(MockDataColumns.PRODUCT_RATING, col(MockDataColumns.PRODUCT_RATING).cast("double"))
                .withColumn(MockDataColumns.PRODUCT_REVIEWS, col(MockDataColumns.PRODUCT_REVIEWS).cast("int"));
    }

    private void validateHeader(List<String> actual) {
        List<String> expected = MockDataColumns.EXPECTED_HEADER;
        if (actual.size() != expected.size()) {
            throw new IllegalArgumentException(
                    "CSV header column count mismatch: expected " + expected.size() + ", got " + actual.size());
        }
        for (int i = 0; i < expected.size(); i++) {
            if (!expected.get(i).equals(actual.get(i))) {
                throw new IllegalArgumentException(
                        "CSV header mismatch at position " + i + ": expected '" + expected.get(i)
                                + "', got '" + actual.get(i) + "'");
            }
        }
    }
}
