package com.bigdataspark.service;

import com.bigdataspark.columns.clickhouse.ClickHouseTables;
import com.bigdataspark.columns.clickhouse.mart.ProductQualityColumns;
import com.bigdataspark.columns.clickhouse.mart.SalesByCustomerColumns;
import com.bigdataspark.columns.clickhouse.mart.SalesByProductColumns;
import com.bigdataspark.columns.clickhouse.mart.SalesByStoreColumns;
import com.bigdataspark.columns.clickhouse.mart.SalesBySupplierColumns;
import com.bigdataspark.columns.clickhouse.mart.SalesByTimeColumns;
import com.bigdataspark.columns.postgres.snowflake.DimAddressColumns;
import com.bigdataspark.columns.postgres.snowflake.DimCityColumns;
import com.bigdataspark.columns.postgres.snowflake.DimCountryColumns;
import com.bigdataspark.columns.postgres.snowflake.DimCustomerColumns;
import com.bigdataspark.columns.postgres.snowflake.DimProductColumns;
import com.bigdataspark.columns.postgres.snowflake.DimStoreColumns;
import com.bigdataspark.columns.postgres.snowflake.DimSupplierColumns;
import com.bigdataspark.columns.postgres.snowflake.FactSalesColumns;
import com.bigdataspark.columns.postgres.snowflake.SnowflakeTables;
import com.bigdataspark.config.db.ClickHouseConnectionProperties;
import com.bigdataspark.config.db.PostgresConnectionProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.util.Properties;

import static org.apache.spark.sql.SaveMode.Append;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.corr;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.month;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.year;

@Service
@RequiredArgsConstructor
@Slf4j
public class SnowflakeToClickHouseEtlService {

    private final SparkSession sparkSession;
    private final PostgresConnectionProperties postgresConnectionProperties;
    private final ClickHouseConnectionProperties clickHouseConnectionProperties;
    private final ClickHouseSchemaService clickHouseSchemaService;

    public void run() {
        log.info("ETL snowflake -> ClickHouse: recreate mart tables");
        clickHouseSchemaService.recreateMartTables();

        String pgUrl = postgresConnectionProperties.url();
        Properties pgProps = postgresConnectionProperties.sparkConnectionProperties();
        Properties chProps = clickHouseConnectionProperties.sparkJdbcProperties();

        Dataset<Row> fact = sparkSession.read().jdbc(pgUrl, SnowflakeTables.FACT_SALES, pgProps);
        Dataset<Row> dimProduct = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_PRODUCT, pgProps);
        Dataset<Row> dimCustomer = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_CUSTOMER, pgProps);
        Dataset<Row> dimStore = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_STORE, pgProps);
        Dataset<Row> dimSupplier = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_SUPPLIER, pgProps);
        Dataset<Row> dimAddress = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_ADDRESS, pgProps);
        Dataset<Row> dimCity = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_CITY, pgProps);
        Dataset<Row> dimCountry = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_COUNTRY, pgProps);

        Dataset<Row> fp = fact.join(dimProduct,
                        fact.col(FactSalesColumns.PRODUCT_ID).equalTo(dimProduct.col(DimProductColumns.PRODUCT_ID)),
                        "left")
                .select(
                        dimProduct.col(DimProductColumns.PRODUCT_ID),
                        dimProduct.col(DimProductColumns.PRODUCT_NAME),
                        dimProduct.col(DimProductColumns.CATEGORY),
                        dimProduct.col(DimProductColumns.RATING),
                        dimProduct.col(DimProductColumns.REVIEWS),
                        fact.col(FactSalesColumns.QUANTITY),
                        fact.col(FactSalesColumns.TOTAL_PRICE)
                );

        log.info("Building sales_by_product (top 10 by revenue)");
        Dataset<Row> salesByProduct = fp.groupBy(
                        col(DimProductColumns.PRODUCT_ID),
                        col(DimProductColumns.PRODUCT_NAME),
                        col(DimProductColumns.CATEGORY))
                .agg(
                        sum(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesByProductColumns.TOTAL_REVENUE),
                        sum(col(FactSalesColumns.QUANTITY)).alias(SalesByProductColumns.TOTAL_QUANTITY),
                        avg(col(DimProductColumns.RATING)).alias(SalesByProductColumns.AVG_RATING),
                        max(col(DimProductColumns.REVIEWS)).alias(SalesByProductColumns.TOTAL_REVIEWS)
                )
                .orderBy(col(SalesByProductColumns.TOTAL_REVENUE).desc())
                .limit(10);
        writeCh(salesByProduct, ClickHouseTables.SALES_BY_PRODUCT, chProps);

        Dataset<Row> custGeo = dimCustomer
                .join(dimAddress,
                        dimCustomer.col(DimCustomerColumns.ADDRESS_ID).equalTo(dimAddress.col(DimAddressColumns.ADDRESS_ID)),
                        "left")
                .join(dimCity,
                        dimAddress.col(DimAddressColumns.CITY_ID).equalTo(dimCity.col(DimCityColumns.CITY_ID)),
                        "left")
                .join(dimCountry,
                        dimCity.col(DimCityColumns.COUNTRY_ID).equalTo(dimCountry.col(DimCountryColumns.COUNTRY_ID)),
                        "left")
                .select(
                        dimCustomer.col(DimCustomerColumns.CUSTOMER_ID),
                        concat_ws(" ",
                                dimCustomer.col(DimCustomerColumns.FIRST_NAME),
                                dimCustomer.col(DimCustomerColumns.LAST_NAME)).alias(SalesByCustomerColumns.FULL_NAME),
                        dimCountry.col(DimCountryColumns.COUNTRY_NAME).alias(SalesByCustomerColumns.COUNTRY)
                );

        final String joinCustId = "_join_customer_id";
        Dataset<Row> fc = fact.join(custGeo,
                        fact.col(FactSalesColumns.CUSTOMER_ID).equalTo(custGeo.col(DimCustomerColumns.CUSTOMER_ID)),
                        "left")
                .select(
                        fact.col(FactSalesColumns.CUSTOMER_ID).alias(joinCustId),
                        fact.col(FactSalesColumns.TOTAL_PRICE),
                        custGeo.col(SalesByCustomerColumns.FULL_NAME),
                        custGeo.col(SalesByCustomerColumns.COUNTRY)
                );

        log.info("Building sales_by_customer (top 10 by spend)");
        Dataset<Row> salesByCustomer = fc.groupBy(
                        col(joinCustId),
                        col(SalesByCustomerColumns.FULL_NAME),
                        col(SalesByCustomerColumns.COUNTRY))
                .agg(
                        sum(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesByCustomerColumns.TOTAL_SPENT),
                        avg(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesByCustomerColumns.AVG_CHECK),
                        count(lit(1)).alias(SalesByCustomerColumns.ORDERS_COUNT)
                )
                .withColumnRenamed(joinCustId, SalesByCustomerColumns.CUSTOMER_ID)
                .orderBy(col(SalesByCustomerColumns.TOTAL_SPENT).desc())
                .limit(10);
        writeCh(salesByCustomer, ClickHouseTables.SALES_BY_CUSTOMER, chProps);

        log.info("Building sales_by_time");
        Dataset<Row> salesByTime = fact
                .withColumn(SalesByTimeColumns.YEAR, year(fact.col(FactSalesColumns.SALE_DATE)))
                .withColumn(SalesByTimeColumns.MONTH, month(fact.col(FactSalesColumns.SALE_DATE)))
                .groupBy(col(SalesByTimeColumns.YEAR), col(SalesByTimeColumns.MONTH))
                .agg(
                        sum(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesByTimeColumns.TOTAL_REVENUE),
                        avg(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesByTimeColumns.AVG_ORDER_VALUE),
                        count(lit(1)).alias(SalesByTimeColumns.ORDERS_COUNT)
                );
        writeCh(salesByTime, ClickHouseTables.SALES_BY_TIME, chProps);

        Dataset<Row> storeGeo = dimStore
                .join(dimAddress,
                        dimStore.col(DimStoreColumns.ADDRESS_ID).equalTo(dimAddress.col(DimAddressColumns.ADDRESS_ID)),
                        "left")
                .join(dimCity,
                        dimAddress.col(DimAddressColumns.CITY_ID).equalTo(dimCity.col(DimCityColumns.CITY_ID)),
                        "left")
                .join(dimCountry,
                        dimCity.col(DimCityColumns.COUNTRY_ID).equalTo(dimCountry.col(DimCountryColumns.COUNTRY_ID)),
                        "left")
                .select(
                        dimStore.col(DimStoreColumns.STORE_ID),
                        dimStore.col(DimStoreColumns.STORE_NAME),
                        dimCity.col(DimCityColumns.CITY_NAME).alias(SalesByStoreColumns.CITY),
                        dimCountry.col(DimCountryColumns.COUNTRY_NAME).alias(SalesByStoreColumns.COUNTRY)
                );

        final String joinStoreId = "_join_store_id";
        Dataset<Row> fs = fact.join(storeGeo,
                        fact.col(FactSalesColumns.STORE_ID).equalTo(storeGeo.col(DimStoreColumns.STORE_ID)),
                        "left")
                .select(
                        fact.col(FactSalesColumns.STORE_ID).alias(joinStoreId),
                        fact.col(FactSalesColumns.TOTAL_PRICE),
                        storeGeo.col(DimStoreColumns.STORE_NAME),
                        storeGeo.col(SalesByStoreColumns.CITY),
                        storeGeo.col(SalesByStoreColumns.COUNTRY)
                );

        log.info("Building sales_by_store (top 5 by revenue)");
        Dataset<Row> salesByStore = fs.groupBy(
                        col(joinStoreId),
                        col(DimStoreColumns.STORE_NAME),
                        col(SalesByStoreColumns.CITY),
                        col(SalesByStoreColumns.COUNTRY))
                .agg(
                        sum(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesByStoreColumns.TOTAL_REVENUE),
                        avg(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesByStoreColumns.AVG_CHECK)
                )
                .withColumnRenamed(joinStoreId, SalesByStoreColumns.STORE_ID)
                .orderBy(col(SalesByStoreColumns.TOTAL_REVENUE).desc())
                .limit(5);
        writeCh(salesByStore, ClickHouseTables.SALES_BY_STORE, chProps);

        Dataset<Row> supGeo = dimSupplier
                .join(dimAddress,
                        dimSupplier.col(DimSupplierColumns.ADDRESS_ID).equalTo(dimAddress.col(DimAddressColumns.ADDRESS_ID)),
                        "left")
                .join(dimCity,
                        dimAddress.col(DimAddressColumns.CITY_ID).equalTo(dimCity.col(DimCityColumns.CITY_ID)),
                        "left")
                .join(dimCountry,
                        dimCity.col(DimCityColumns.COUNTRY_ID).equalTo(dimCountry.col(DimCountryColumns.COUNTRY_ID)),
                        "left")
                .select(
                        dimSupplier.col(DimSupplierColumns.SUPPLIER_ID),
                        dimSupplier.col(DimSupplierColumns.SUPPLIER_NAME),
                        dimCountry.col(DimCountryColumns.COUNTRY_NAME).alias(SalesBySupplierColumns.COUNTRY)
                );

        final String joinSupId = "_join_supplier_id";
        Dataset<Row> fSup = fact.join(dimProduct,
                        fact.col(FactSalesColumns.PRODUCT_ID).equalTo(dimProduct.col(DimProductColumns.PRODUCT_ID)),
                        "left")
                .join(supGeo,
                        dimProduct.col(DimProductColumns.SUPPLIER_ID).equalTo(supGeo.col(DimSupplierColumns.SUPPLIER_ID)),
                        "left")
                .select(
                        fact.col(FactSalesColumns.TOTAL_PRICE),
                        dimProduct.col(DimProductColumns.PRICE),
                        supGeo.col(DimSupplierColumns.SUPPLIER_ID).alias(joinSupId),
                        supGeo.col(DimSupplierColumns.SUPPLIER_NAME),
                        supGeo.col(SalesBySupplierColumns.COUNTRY)
                );

        log.info("Building sales_by_supplier (top 5 by revenue)");
        Dataset<Row> salesBySupplier = fSup.groupBy(
                        col(joinSupId),
                        col(DimSupplierColumns.SUPPLIER_NAME),
                        col(SalesBySupplierColumns.COUNTRY))
                .agg(
                        sum(col(FactSalesColumns.TOTAL_PRICE)).alias(SalesBySupplierColumns.TOTAL_REVENUE),
                        avg(col(DimProductColumns.PRICE)).alias(SalesBySupplierColumns.AVG_PRODUCT_PRICE)
                )
                .withColumnRenamed(joinSupId, SalesBySupplierColumns.SUPPLIER_ID)
                .orderBy(col(SalesBySupplierColumns.TOTAL_REVENUE).desc())
                .limit(5);
        writeCh(salesBySupplier, ClickHouseTables.SALES_BY_SUPPLIER, chProps);

        log.info("Building product_quality");
        Dataset<Row> perProduct = fp.groupBy(
                        col(DimProductColumns.PRODUCT_ID),
                        col(DimProductColumns.PRODUCT_NAME))
                .agg(
                        max(col(DimProductColumns.RATING)).alias(ProductQualityColumns.RATING),
                        max(col(DimProductColumns.REVIEWS)).alias(ProductQualityColumns.REVIEWS_COUNT),
                        sum(col(FactSalesColumns.QUANTITY)).alias(ProductQualityColumns.TOTAL_SOLD)
                );

        Row corrRow = perProduct.agg(
                corr(col(ProductQualityColumns.RATING), col(ProductQualityColumns.TOTAL_SOLD))
        ).first();
        double corrVal = 0.0;
        if (corrRow != null && !corrRow.isNullAt(0)) {
            double v = corrRow.getDouble(0);
            if (!Double.isNaN(v)) {
                corrVal = v;
            }
        }
        Dataset<Row> productQuality = perProduct.withColumn(
                ProductQualityColumns.CORRELATION_RATING_SALES,
                lit(corrVal)
        );
        writeCh(productQuality, ClickHouseTables.PRODUCT_QUALITY, chProps);

        log.info("ETL snowflake -> ClickHouse: completed");
    }

    private void writeCh(Dataset<Row> df, String fqTable, Properties chProps) {
        String url = clickHouseConnectionProperties.url();
        String table = fqTable.contains(".") ? fqTable.substring(fqTable.indexOf('.') + 1) : fqTable;
        df.write()
                .mode(Append)
                .jdbc(url, table, chProps);
    }
}
