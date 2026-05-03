package com.bigdataspark.service;

import com.bigdataspark.columns.postgres.MockDataColumns;
import com.bigdataspark.columns.postgres.PublicTables;
import com.bigdataspark.columns.postgres.snowflake.DimAddressColumns;
import com.bigdataspark.columns.postgres.snowflake.DimCityColumns;
import com.bigdataspark.columns.postgres.snowflake.DimCountryColumns;
import com.bigdataspark.columns.postgres.snowflake.DimCustomerColumns;
import com.bigdataspark.columns.postgres.snowflake.DimPetColumns;
import com.bigdataspark.columns.postgres.snowflake.DimProductColumns;
import com.bigdataspark.columns.postgres.snowflake.DimSellerColumns;
import com.bigdataspark.columns.postgres.snowflake.DimStoreColumns;
import com.bigdataspark.columns.postgres.snowflake.DimSupplierColumns;
import com.bigdataspark.columns.postgres.snowflake.SnowflakeTables;
import com.bigdataspark.columns.postgres.staging.StagingAddressColumns;
import com.bigdataspark.columns.postgres.staging.StagingCityColumns;
import com.bigdataspark.columns.postgres.staging.StagingCountryColumns;
import com.bigdataspark.columns.postgres.staging.StagingFactColumns;
import com.bigdataspark.columns.postgres.staging.StagingProductColumns;
import com.bigdataspark.columns.postgres.staging.StagingSupplierColumns;
import com.bigdataspark.config.db.PostgresConnectionProperties;
import com.bigdataspark.constants.EtlConstants;
import com.bigdataspark.persistence.query.postgres.merge.MergeQuery;
import com.bigdataspark.persistence.repository.merge.MergeRepository;
import com.bigdataspark.persistence.repository.staging.StagingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import java.util.Properties;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

@Service
@RequiredArgsConstructor
@Slf4j
public class MockToSnowflakeEtlService {

    private final SparkSession sparkSession;
    private final PostgresConnectionProperties postgresConnectionProperties;
    private final StagingRepository stagingRepository;
    private final MergeRepository mergeRepository;
    private final FactSalesLoadService factSalesLoadService;

    public void run() {
        log.info("ETL mock_data -> snowflake: preparing staging tables");
        stagingRepository.dropStagingTables();
        stagingRepository.createStagingTables();

        String pgUrl = postgresConnectionProperties.url();
        Properties props = postgresConnectionProperties.sparkConnectionProperties();

        Dataset<Row> mock = sparkSession.read()
                .jdbc(pgUrl, "(SELECT * FROM " + PublicTables.MOCK_DATA + ") md", props);

        log.info("ETL: countries -> staging -> MERGE dim_country");
        writeCountries(mock, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_COUNTRY);

        log.info("ETL: cities -> staging -> MERGE dim_city");
        writeCities(mock, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_CITY);

        Dataset<Row> cityMap = readCityMap(pgUrl, props);
        Dataset<Row> addressTable = sparkSession.read()
                .jdbc(pgUrl,
                        "(SELECT " + DimAddressColumns.ADDRESS_ID + ", "
                                + DimAddressColumns.POSTAL_CODE + ", "
                                + DimAddressColumns.CITY_ID
                                + " FROM " + SnowflakeTables.DIM_ADDRESS + ") a",
                        props);

        log.info("ETL: addresses -> staging -> MERGE dim_address");
        writeAddresses(mock, cityMap, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_ADDRESS);

        addressTable = sparkSession.read()
                .jdbc(pgUrl,
                        "(SELECT " + DimAddressColumns.ADDRESS_ID + ", "
                                + DimAddressColumns.POSTAL_CODE + ", "
                                + DimAddressColumns.CITY_ID
                                + " FROM " + SnowflakeTables.DIM_ADDRESS + ") a",
                        props);

        log.info("ETL: suppliers -> staging -> MERGE dim_supplier");
        writeSuppliers(mock, cityMap, addressTable, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_SUPPLIER);

        Dataset<Row> dimSupplier = sparkSession.read()
                .jdbc(pgUrl, SnowflakeTables.DIM_SUPPLIER, props)
                .select(
                        col(DimSupplierColumns.SUPPLIER_ID),
                        col(DimSupplierColumns.SUPPLIER_NAME).as("ds_supplier_name"),
                        col(DimSupplierColumns.EMAIL).as("ds_supplier_email")
                );

        log.info("ETL: products -> staging -> MERGE dim_product");
        writeProducts(mock, dimSupplier, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_PRODUCT);

        log.info("ETL: customers -> staging -> MERGE dim_customer");
        writeCustomers(mock, cityMap, addressTable, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_CUSTOMER);

        log.info("ETL: sellers -> staging -> MERGE dim_seller");
        writeSellers(mock, cityMap, addressTable, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_SELLER);

        log.info("ETL: stores -> staging -> MERGE dim_store");
        writeStores(mock, cityMap, addressTable, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_STORE);

        log.info("ETL: pets -> staging -> MERGE dim_pet");
        writePets(mock, pgUrl, props);
        mergeRepository.executeMerge(MergeQuery.MERGE_DIM_PET);

        log.info("ETL: fact staging -> transactional load into fact_sales");
        writeFacts(mock, pgUrl, props);
        factSalesLoadService.truncateAndInsertFromStaging();

        log.info("ETL mock_data -> snowflake: completed");
    }

    private void writeCountries(Dataset<Row> mock, String pgUrl, Properties props) {
        Dataset<Row> d1 = mock.select(col(MockDataColumns.CUSTOMER_COUNTRY).as(StagingCountryColumns.COUNTRY_NAME))
                .where(col(MockDataColumns.CUSTOMER_COUNTRY).isNotNull());
        Dataset<Row> d2 = mock.select(col(MockDataColumns.SELLER_COUNTRY).as(StagingCountryColumns.COUNTRY_NAME))
                .where(col(MockDataColumns.SELLER_COUNTRY).isNotNull());
        Dataset<Row> d3 = mock.select(col(MockDataColumns.STORE_COUNTRY).as(StagingCountryColumns.COUNTRY_NAME))
                .where(col(MockDataColumns.STORE_COUNTRY).isNotNull());
        Dataset<Row> d4 = mock.select(col(MockDataColumns.SUPPLIER_COUNTRY).as(StagingCountryColumns.COUNTRY_NAME))
                .where(col(MockDataColumns.SUPPLIER_COUNTRY).isNotNull());
        appendToPg(d1.union(d2).union(d3).union(d4).distinct(), pgUrl, props, PublicTables.ETL_STG_COUNTRY);
    }

    private void writeCities(Dataset<Row> mock, String pgUrl, Properties props) {
        Dataset<Row> c1 = mock.select(
                lit(EtlConstants.UNKNOWN_CITY).as(StagingCityColumns.CITY_NAME),
                col(MockDataColumns.CUSTOMER_COUNTRY).as(StagingCityColumns.COUNTRY_NAME)
        ).where(col(MockDataColumns.CUSTOMER_COUNTRY).isNotNull());

        Dataset<Row> c2 = mock.select(
                lit(EtlConstants.UNKNOWN_CITY).as(StagingCityColumns.CITY_NAME),
                col(MockDataColumns.SELLER_COUNTRY).as(StagingCityColumns.COUNTRY_NAME)
        ).where(col(MockDataColumns.SELLER_COUNTRY).isNotNull());

        Dataset<Row> c3 = mock.select(
                col(MockDataColumns.STORE_CITY).as(StagingCityColumns.CITY_NAME),
                col(MockDataColumns.STORE_COUNTRY).as(StagingCityColumns.COUNTRY_NAME)
        ).where(col(MockDataColumns.STORE_CITY).isNotNull()
                .and(col(MockDataColumns.STORE_COUNTRY).isNotNull()));

        Dataset<Row> c4 = mock.select(
                col(MockDataColumns.SUPPLIER_CITY).as(StagingCityColumns.CITY_NAME),
                col(MockDataColumns.SUPPLIER_COUNTRY).as(StagingCityColumns.COUNTRY_NAME)
        ).where(col(MockDataColumns.SUPPLIER_CITY).isNotNull()
                .and(col(MockDataColumns.SUPPLIER_COUNTRY).isNotNull()));

        appendToPg(c1.union(c2).union(c3).union(c4).distinct(), pgUrl, props, PublicTables.ETL_STG_CITY);
    }

    private Dataset<Row> readCityMap(String pgUrl, Properties props) {
        return sparkSession.read()
                .jdbc(pgUrl,
                        "(SELECT ci." + DimCityColumns.CITY_ID + " AS map_city_id, ci."
                                + DimCityColumns.CITY_NAME + " AS map_city_name, co."
                                + DimCountryColumns.COUNTRY_NAME + " AS map_country "
                                + "FROM " + SnowflakeTables.DIM_CITY + " ci JOIN "
                                + SnowflakeTables.DIM_COUNTRY + " co ON ci."
                                + DimCityColumns.COUNTRY_ID + " = co." + DimCountryColumns.COUNTRY_ID + ") cm",
                        props);
    }

    private void writeAddresses(Dataset<Row> mock, Dataset<Row> cityMap, String pgUrl, Properties props) {
        Dataset<Row> a1 = addressFromPerson(mock, cityMap,
                MockDataColumns.CUSTOMER_COUNTRY, MockDataColumns.CUSTOMER_POSTAL_CODE);
        Dataset<Row> a2 = addressFromPerson(mock, cityMap,
                MockDataColumns.SELLER_COUNTRY, MockDataColumns.SELLER_POSTAL_CODE);
        Dataset<Row> a3 = addressFromStore(mock, cityMap);
        Dataset<Row> a4 = addressFromSupplier(mock, cityMap);
        appendToPg(a1.union(a2).union(a3).union(a4).distinct(), pgUrl, props, PublicTables.ETL_STG_ADDRESS);
    }

    private Dataset<Row> addressFromPerson(Dataset<Row> mock, Dataset<Row> cityMap,
                                           String countryCol, String postalCol) {
        return mock.join(cityMap,
                        col(countryCol).equalTo(col("map_country"))
                                .and(col("map_city_name").equalTo(lit(EtlConstants.UNKNOWN_CITY))),
                        "inner")
                .select(
                        normPostal(col(postalCol)).as(StagingAddressColumns.POSTAL_CODE),
                        col("map_city_id").cast("int").as(StagingAddressColumns.CITY_ID)
                ).distinct();
    }

    private Dataset<Row> addressFromStore(Dataset<Row> mock, Dataset<Row> cityMap) {
        Column storePostal = when(col(MockDataColumns.STORE_LOCATION).isNull()
                                .or(col(MockDataColumns.STORE_LOCATION).equalTo(lit(""))),
                        lit(EtlConstants.MISSING_POSTAL))
                .otherwise(substring(col(MockDataColumns.STORE_LOCATION), 1, 50));
        return mock.join(cityMap,
                        col(MockDataColumns.STORE_CITY).equalTo(col("map_city_name"))
                                .and(col(MockDataColumns.STORE_COUNTRY).equalTo(col("map_country"))),
                        "inner")
                .select(
                        storePostal.as(StagingAddressColumns.POSTAL_CODE),
                        col("map_city_id").cast("int").as(StagingAddressColumns.CITY_ID)
                ).distinct();
    }

    private Dataset<Row> addressFromSupplier(Dataset<Row> mock, Dataset<Row> cityMap) {
        Column supPostal = when(col(MockDataColumns.SUPPLIER_ADDRESS).isNull()
                                .or(col(MockDataColumns.SUPPLIER_ADDRESS).equalTo(lit(""))),
                        lit(EtlConstants.MISSING_POSTAL))
                .otherwise(substring(col(MockDataColumns.SUPPLIER_ADDRESS), 1, 50));
        return mock.join(cityMap,
                        col(MockDataColumns.SUPPLIER_CITY).equalTo(col("map_city_name"))
                                .and(col(MockDataColumns.SUPPLIER_COUNTRY).equalTo(col("map_country"))),
                        "inner")
                .select(
                        supPostal.as(StagingAddressColumns.POSTAL_CODE),
                        col("map_city_id").cast("int").as(StagingAddressColumns.CITY_ID)
                ).distinct();
    }

    private Column normPostal(Column postalCol) {
        return when(postalCol.isNull().or(postalCol.equalTo(lit(""))),
                lit(EtlConstants.MISSING_POSTAL)).otherwise(substring(postalCol, 1, 50));
    }

    private void writeSuppliers(Dataset<Row> mock, Dataset<Row> cityMap, Dataset<Row> addressTable,
                                String pgUrl, Properties props) {
        Column supPostal = when(col(MockDataColumns.SUPPLIER_ADDRESS).isNull()
                        .or(col(MockDataColumns.SUPPLIER_ADDRESS).equalTo(lit(""))),
                lit(EtlConstants.MISSING_POSTAL))
                .otherwise(substring(col(MockDataColumns.SUPPLIER_ADDRESS), 1, 50));
        Dataset<Row> base = mock.select(
                col(MockDataColumns.SUPPLIER_NAME),
                col(MockDataColumns.SUPPLIER_CONTACT),
                col(MockDataColumns.SUPPLIER_EMAIL),
                col(MockDataColumns.SUPPLIER_PHONE),
                supPostal.as("sup_postal"),
                col(MockDataColumns.SUPPLIER_CITY),
                col(MockDataColumns.SUPPLIER_COUNTRY)
        ).where(col(MockDataColumns.SUPPLIER_NAME).isNotNull());

        Dataset<Row> withCity = base.join(cityMap,
                col(MockDataColumns.SUPPLIER_CITY).equalTo(col("map_city_name"))
                        .and(col(MockDataColumns.SUPPLIER_COUNTRY).equalTo(col("map_country"))),
                "inner");

        Dataset<Row> withAddr = withCity.join(addressTable,
                col("map_city_id").equalTo(col(DimAddressColumns.CITY_ID))
                        .and(col("sup_postal").equalTo(col(DimAddressColumns.POSTAL_CODE))),
                "inner");

        Dataset<Row> stg = withAddr.select(
                substring(col(MockDataColumns.SUPPLIER_NAME), 1, 200).as(StagingSupplierColumns.SUPPLIER_NAME),
                substring(col(MockDataColumns.SUPPLIER_CONTACT), 1, 100).as(StagingSupplierColumns.CONTACT),
                col(MockDataColumns.SUPPLIER_EMAIL).as(StagingSupplierColumns.EMAIL),
                substring(col(MockDataColumns.SUPPLIER_PHONE), 1, 50).as(StagingSupplierColumns.PHONE),
                col(DimAddressColumns.ADDRESS_ID).as(StagingSupplierColumns.ADDRESS_ID)
        ).distinct();

        appendToPg(stg, pgUrl, props, PublicTables.ETL_STG_SUPPLIER);
    }

    private void writeProducts(Dataset<Row> mock, Dataset<Row> dimSupplier, String pgUrl, Properties props) {
        Dataset<Row> joined = mock.join(dimSupplier,
                col(MockDataColumns.SUPPLIER_NAME).equalTo(col("ds_supplier_name"))
                        .and(col(MockDataColumns.SUPPLIER_EMAIL).equalTo(col("ds_supplier_email"))),
                "inner");

        Column rel = coalesce(
                to_date(col(MockDataColumns.PRODUCT_RELEASE_DATE), "M/d/yyyy"),
                to_date(col(MockDataColumns.PRODUCT_RELEASE_DATE), "yyyy-MM-dd")
        );
        Column exp = coalesce(
                to_date(col(MockDataColumns.PRODUCT_EXPIRY_DATE), "M/d/yyyy"),
                to_date(col(MockDataColumns.PRODUCT_EXPIRY_DATE), "yyyy-MM-dd")
        );

        Dataset<Row> stg = joined.select(
                substring(col(MockDataColumns.PRODUCT_NAME), 1, 50).as(StagingProductColumns.PRODUCT_NAME),
                substring(col(MockDataColumns.PRODUCT_CATEGORY), 1, 50).as(StagingProductColumns.CATEGORY),
                col(MockDataColumns.PRODUCT_PRICE).cast("float").as(StagingProductColumns.PRICE),
                col(MockDataColumns.PRODUCT_WEIGHT).cast("float").as(StagingProductColumns.WEIGHT),
                substring(col(MockDataColumns.PRODUCT_COLOR), 1, 50).as(StagingProductColumns.COLOR),
                substring(col(MockDataColumns.PRODUCT_SIZE), 1, 50).as(StagingProductColumns.SIZE),
                substring(col(MockDataColumns.PRODUCT_BRAND), 1, 50).as(StagingProductColumns.BRAND),
                substring(col(MockDataColumns.PRODUCT_MATERIAL), 1, 50).as(StagingProductColumns.MATERIAL),
                substring(col(MockDataColumns.PRODUCT_DESCRIPTION), 1, 1024).as(StagingProductColumns.DESCRIPTION),
                col(MockDataColumns.PRODUCT_RATING).cast("float").as(StagingProductColumns.RATING),
                col(MockDataColumns.PRODUCT_REVIEWS).cast("int").as(StagingProductColumns.REVIEWS),
                rel.cast("string").as(StagingProductColumns.RELEASE_DATE),
                exp.cast("string").as(StagingProductColumns.EXPIRY_DATE),
                col(DimSupplierColumns.SUPPLIER_ID).as(StagingProductColumns.SUPPLIER_ID)
        ).distinct();

        appendToPg(stg, pgUrl, props, PublicTables.ETL_STG_PRODUCT);
    }

    private void writeCustomers(Dataset<Row> mock, Dataset<Row> cityMap, Dataset<Row> addressTable,
                                String pgUrl, Properties props) {
        Dataset<Row> withCity = mock.join(cityMap,
                col(MockDataColumns.CUSTOMER_COUNTRY).equalTo(col("map_country"))
                        .and(col("map_city_name").equalTo(lit(EtlConstants.UNKNOWN_CITY))),
                "inner");
        Column pc = normPostal(col(MockDataColumns.CUSTOMER_POSTAL_CODE));
        Dataset<Row> withAddr = withCity.join(addressTable,
                col("map_city_id").equalTo(col(DimAddressColumns.CITY_ID))
                        .and(pc.equalTo(col(DimAddressColumns.POSTAL_CODE))),
                "inner");

        Dataset<Row> stg = withAddr.select(
                substring(col(MockDataColumns.CUSTOMER_FIRST_NAME), 1, 50).as(DimCustomerColumns.FIRST_NAME),
                substring(col(MockDataColumns.CUSTOMER_LAST_NAME), 1, 50).as(DimCustomerColumns.LAST_NAME),
                col(MockDataColumns.CUSTOMER_AGE).cast("int").as(DimCustomerColumns.AGE),
                substring(col(MockDataColumns.CUSTOMER_EMAIL), 1, 50).as(DimCustomerColumns.EMAIL),
                col(DimAddressColumns.ADDRESS_ID).as(DimCustomerColumns.ADDRESS_ID)
        ).dropDuplicates(DimCustomerColumns.EMAIL);

        appendToPg(stg, pgUrl, props, PublicTables.ETL_STG_CUSTOMER);
    }

    private void writeSellers(Dataset<Row> mock, Dataset<Row> cityMap, Dataset<Row> addressTable,
                              String pgUrl, Properties props) {
        Dataset<Row> withCity = mock.join(cityMap,
                col(MockDataColumns.SELLER_COUNTRY).equalTo(col("map_country"))
                        .and(col("map_city_name").equalTo(lit(EtlConstants.UNKNOWN_CITY))),
                "inner");
        Column pc = normPostal(col(MockDataColumns.SELLER_POSTAL_CODE));
        Dataset<Row> withAddr = withCity.join(addressTable,
                col("map_city_id").equalTo(col(DimAddressColumns.CITY_ID))
                        .and(pc.equalTo(col(DimAddressColumns.POSTAL_CODE))),
                "inner");

        Dataset<Row> stg = withAddr.select(
                substring(col(MockDataColumns.SELLER_FIRST_NAME), 1, 50).as(DimSellerColumns.FIRST_NAME),
                substring(col(MockDataColumns.SELLER_LAST_NAME), 1, 50).as(DimSellerColumns.LAST_NAME),
                substring(col(MockDataColumns.SELLER_EMAIL), 1, 50).as(DimSellerColumns.EMAIL),
                col(DimAddressColumns.ADDRESS_ID).as(DimSellerColumns.ADDRESS_ID)
        ).dropDuplicates(DimSellerColumns.EMAIL);

        appendToPg(stg, pgUrl, props, PublicTables.ETL_STG_SELLER);
    }

    private void writeStores(Dataset<Row> mock, Dataset<Row> cityMap, Dataset<Row> addressTable,
                             String pgUrl, Properties props) {
        Column storePostal = when(col(MockDataColumns.STORE_LOCATION).isNull()
                                .or(col(MockDataColumns.STORE_LOCATION).equalTo(lit(""))),
                        lit(EtlConstants.MISSING_POSTAL))
                .otherwise(substring(col(MockDataColumns.STORE_LOCATION), 1, 50));
        Dataset<Row> withCity = mock.join(cityMap,
                col(MockDataColumns.STORE_CITY).equalTo(col("map_city_name"))
                        .and(col(MockDataColumns.STORE_COUNTRY).equalTo(col("map_country"))),
                "inner");
        Dataset<Row> withAddr = withCity.join(addressTable,
                col("map_city_id").equalTo(col(DimAddressColumns.CITY_ID))
                        .and(storePostal.equalTo(col(DimAddressColumns.POSTAL_CODE))),
                "inner");

        Dataset<Row> stg = withAddr.select(
                substring(col(MockDataColumns.STORE_NAME), 1, 50).as(DimStoreColumns.STORE_NAME),
                col(DimAddressColumns.ADDRESS_ID).as(DimStoreColumns.ADDRESS_ID),
                substring(col(MockDataColumns.STORE_PHONE), 1, 50).as(DimStoreColumns.PHONE),
                substring(col(MockDataColumns.STORE_EMAIL), 1, 50).as(DimStoreColumns.EMAIL)
        ).dropDuplicates(DimStoreColumns.STORE_NAME, DimStoreColumns.EMAIL, DimStoreColumns.PHONE);

        appendToPg(stg, pgUrl, props, PublicTables.ETL_STG_STORE);
    }

    private void writePets(Dataset<Row> mock, String pgUrl, Properties props) {
        Dataset<Row> stg = mock.select(
                substring(col(MockDataColumns.CUSTOMER_PET_NAME), 1, 50).as(DimPetColumns.PET_NAME),
                substring(col(MockDataColumns.CUSTOMER_PET_TYPE), 1, 50).as(DimPetColumns.PET_TYPE),
                substring(col(MockDataColumns.CUSTOMER_PET_BREED), 1, 50).as(DimPetColumns.PET_BREED),
                substring(col(MockDataColumns.PET_CATEGORY), 1, 50).as(DimPetColumns.PET_CATEGORY)
        ).distinct();
        appendToPg(stg, pgUrl, props, PublicTables.ETL_STG_PET);
    }

    private void writeFacts(Dataset<Row> mock, String pgUrl, Properties props) {
        Dataset<Row> dc = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_CUSTOMER, props);
        Dataset<Row> dp = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_PET, props)
                .select(
                        col(DimPetColumns.PET_ID),
                        col(DimPetColumns.PET_NAME).as("dim_pet_name"),
                        col(DimPetColumns.PET_TYPE).as("dim_pet_type"),
                        col(DimPetColumns.PET_BREED).as("dim_pet_breed"),
                        col(DimPetColumns.PET_CATEGORY).as("dim_pet_category"));
        Dataset<Row> ds = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_SELLER, props);
        Dataset<Row> dst = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_STORE, props)
                .select(
                        col(DimStoreColumns.STORE_ID),
                        col(DimStoreColumns.STORE_NAME).as("dim_store_name"),
                        col(DimStoreColumns.ADDRESS_ID),
                        col(DimStoreColumns.PHONE).as("dim_store_phone"),
                        col(DimStoreColumns.EMAIL).as("dim_store_email"));
        Dataset<Row> dpr = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_PRODUCT, props)
                .select(
                        col(DimProductColumns.PRODUCT_ID),
                        col(DimProductColumns.PRODUCT_NAME).as("dim_product_name"),
                        col(DimProductColumns.CATEGORY).as("dim_product_category"),
                        col(DimProductColumns.BRAND).as("dim_product_brand"),
                        col(DimProductColumns.SUPPLIER_ID));
        Dataset<Row> dsup = sparkSession.read().jdbc(pgUrl, SnowflakeTables.DIM_SUPPLIER, props)
                .select(
                        col(DimSupplierColumns.SUPPLIER_ID).as("p_supplier_id"),
                        col(DimSupplierColumns.SUPPLIER_NAME).as("p_supplier_name"),
                        col(DimSupplierColumns.EMAIL).as("p_supplier_email")
                );

        Column kEmailCust = substring(col(MockDataColumns.CUSTOMER_EMAIL), 1, 50);
        Column kEmailSell = substring(col(MockDataColumns.SELLER_EMAIL), 1, 50);
        Column kStoreName = substring(col(MockDataColumns.STORE_NAME), 1, 50);
        Column kStoreEmail = substring(col(MockDataColumns.STORE_EMAIL), 1, 50);
        Column kStorePhone = substring(col(MockDataColumns.STORE_PHONE), 1, 50);
        Column kProdName = substring(col(MockDataColumns.PRODUCT_NAME), 1, 50);
        Column kProdCat = substring(col(MockDataColumns.PRODUCT_CATEGORY), 1, 50);
        Column kProdBrand = substring(col(MockDataColumns.PRODUCT_BRAND), 1, 50);
        Column kPetName = substring(col(MockDataColumns.CUSTOMER_PET_NAME), 1, 50);
        Column kPetType = substring(col(MockDataColumns.CUSTOMER_PET_TYPE), 1, 50);
        Column kPetBreed = substring(col(MockDataColumns.CUSTOMER_PET_BREED), 1, 50);
        Column kPetCat = substring(col(MockDataColumns.PET_CATEGORY), 1, 50);

        Dataset<Row> m = mock
                .join(dc, kEmailCust.equalTo(dc.col(DimCustomerColumns.EMAIL)), "inner")
                .join(dp,
                        kPetName.eqNullSafe(dp.col("dim_pet_name"))
                                .and(kPetType.eqNullSafe(dp.col("dim_pet_type")))
                                .and(kPetBreed.eqNullSafe(dp.col("dim_pet_breed")))
                                .and(kPetCat.eqNullSafe(dp.col("dim_pet_category"))),
                        "inner")
                .join(ds, kEmailSell.equalTo(ds.col(DimSellerColumns.EMAIL)), "inner")
                .join(dst,
                        kStoreName.equalTo(dst.col("dim_store_name"))
                                .and(kStoreEmail.equalTo(dst.col("dim_store_email")))
                                .and(kStorePhone.equalTo(dst.col("dim_store_phone"))),
                        "inner")
                .join(dsup,
                        col(MockDataColumns.SUPPLIER_NAME).equalTo(col("p_supplier_name"))
                                .and(col(MockDataColumns.SUPPLIER_EMAIL).equalTo(col("p_supplier_email"))),
                        "inner")
                .join(dpr,
                        kProdName.equalTo(dpr.col("dim_product_name"))
                                .and(kProdCat.equalTo(dpr.col("dim_product_category")))
                                .and(kProdBrand.eqNullSafe(dpr.col("dim_product_brand")))
                                .and(col("p_supplier_id").equalTo(dpr.col(DimProductColumns.SUPPLIER_ID))),
                        "inner");

        Column saleDate = coalesce(
                to_date(col(MockDataColumns.SALE_DATE), "M/d/yyyy"),
                to_date(col(MockDataColumns.SALE_DATE), "MM/dd/yyyy"),
                to_date(col(MockDataColumns.SALE_DATE), "yyyy-MM-dd")
        );

        Dataset<Row> facts = m.select(
                dc.col(DimCustomerColumns.CUSTOMER_ID).as(StagingFactColumns.CUSTOMER_ID),
                dp.col(DimPetColumns.PET_ID).as(StagingFactColumns.PET_ID),
                ds.col(DimSellerColumns.SELLER_ID).as(StagingFactColumns.SELLER_ID),
                dst.col(DimStoreColumns.STORE_ID).as(StagingFactColumns.STORE_ID),
                dpr.col(DimProductColumns.PRODUCT_ID).as(StagingFactColumns.PRODUCT_ID),
                saleDate.cast("string").as(StagingFactColumns.SALE_DATE),
                col(MockDataColumns.SALE_QUANTITY).cast("int").as(StagingFactColumns.QUANTITY),
                col(MockDataColumns.SALE_TOTAL_PRICE).cast("float").as(StagingFactColumns.TOTAL_PRICE)
        );

        appendToPg(facts, pgUrl, props, PublicTables.ETL_STG_FACT);
    }

    private void appendToPg(Dataset<Row> df, String pgUrl, Properties props, String table) {
        df.write().mode(org.apache.spark.sql.SaveMode.Append).jdbc(pgUrl, table, props);
    }
}
