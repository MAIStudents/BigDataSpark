#!/bin/bash

# Скрипт для проверки всей системы

echo "=========================================="
echo "Проверка системы BigDataSpark Lab #2"
echo "=========================================="

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для проверки статуса
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1${NC}"
        return 0
    else
        echo -e "${RED}✗ $1${NC}"
        return 1
    fi
}

echo ""
echo "1. Проверка Docker контейнеров..."
echo "-----------------------------------"

containers=("postgres" "spark" "clickhouse" "cassandra" "neo4j" "mongodb" "valkey")
for container in "${containers[@]}"; do
    if docker ps | grep -q $container; then
        echo -e "${GREEN}✓ $container запущен${NC}"
    else
        echo -e "${RED}✗ $container не запущен${NC}"
    fi
done

echo ""
echo "2. Проверка PostgreSQL..."
echo "-----------------------------------"

# Проверка исходных данных
count=$(docker exec postgres psql -U user -d bigdata -t -c "SELECT COUNT(*) FROM mock_data" 2>/dev/null | tr -d ' ')
if [ "$count" = "10000" ]; then
    echo -e "${GREEN}✓ mock_data: 10000 записей${NC}"
else
    echo -e "${YELLOW}⚠ mock_data: $count записей (ожидается 10000)${NC}"
fi

# Проверка таблиц измерений
tables=("dim_customers" "dim_sellers" "dim_products" "dim_stores" "dim_suppliers" "dim_time" "fact_sales")
for table in "${tables[@]}"; do
    count=$(docker exec postgres psql -U user -d bigdata -t -c "SELECT COUNT(*) FROM $table" 2>/dev/null | tr -d ' ')
    if [ ! -z "$count" ] && [ "$count" -gt 0 ]; then
        echo -e "${GREEN}✓ $table: $count записей${NC}"
    else
        echo -e "${RED}✗ $table: таблица пуста или не существует${NC}"
    fi
done

echo ""
echo "3. Проверка ClickHouse..."
echo "-----------------------------------"

ch_tables=("product_sales_report" "customer_sales_report" "time_sales_report" "store_sales_report" "supplier_sales_report" "product_quality_report")
for table in "${ch_tables[@]}"; do
    count=$(docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM $table" 2>/dev/null)
    if [ ! -z "$count" ] && [ "$count" -gt 0 ]; then
        echo -e "${GREEN}✓ $table: $count записей${NC}"
    else
        echo -e "${YELLOW}⚠ $table: таблица пуста или не существует${NC}"
    fi
done

echo ""
echo "4. Проверка Cassandra..."
echo "-----------------------------------"

cass_check=$(docker exec cassandra cqlsh -e "USE bigdata; DESCRIBE TABLES;" 2>/dev/null)
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Cassandra keyspace 'bigdata' существует${NC}"
    for table in "${ch_tables[@]}"; do
        if echo "$cass_check" | grep -q "$table"; then
            echo -e "${GREEN}✓ $table существует${NC}"
        else
            echo -e "${YELLOW}⚠ $table не найдена${NC}"
        fi
    done
else
    echo -e "${YELLOW}⚠ Cassandra keyspace 'bigdata' не найден${NC}"
fi

echo ""
echo "5. Проверка Neo4j..."
echo "-----------------------------------"

neo4j_labels=("ProductSalesReport" "CustomerSalesReport" "TimeSalesReport" "StoreSalesReport" "SupplierSalesReport" "ProductQualityReport")
echo -e "${YELLOW}ℹ Для проверки Neo4j откройте http://localhost:7474${NC}"
echo -e "${YELLOW}ℹ Логин: neo4j, Пароль: password${NC}"

echo ""
echo "6. Проверка MongoDB..."
echo "-----------------------------------"

mongo_collections=("product_sales_report" "customer_sales_report" "time_sales_report" "store_sales_report" "supplier_sales_report" "product_quality_report")
for collection in "${mongo_collections[@]}"; do
    count=$(docker exec mongodb mongosh --quiet --eval "db.getSiblingDB('bigdata').$collection.countDocuments()" 2>/dev/null)
    if [ ! -z "$count" ] && [ "$count" -gt 0 ]; then
        echo -e "${GREEN}✓ $collection: $count документов${NC}"
    else
        echo -e "${YELLOW}⚠ $collection: коллекция пуста или не существует${NC}"
    fi
done

echo ""
echo "7. Проверка Valkey (Redis)..."
echo "-----------------------------------"

valkey_keys=$(docker exec valkey redis-cli KEYS "*" 2>/dev/null | wc -l)
if [ "$valkey_keys" -gt 0 ]; then
    echo -e "${GREEN}✓ Valkey содержит $valkey_keys ключей${NC}"
else
    echo -e "${YELLOW}⚠ Valkey не содержит ключей${NC}"
fi

echo ""
echo "=========================================="
echo "Проверка завершена!"
echo "=========================================="
echo ""
echo "Доступ к сервисам:"
echo "- PostgreSQL: localhost:5432 (user/password)"
echo "- Spark Jupyter: http://localhost:8888"
echo "- ClickHouse: localhost:8123, localhost:9000"
echo "- Cassandra: localhost:9042"
echo "- Neo4j: http://localhost:7474 (neo4j/password)"
echo "- MongoDB: localhost:27017"
echo "- Valkey: localhost:6379"
echo ""
echo "Для подробных запросов см. файлы:"
echo "- verification_queries.sql (PostgreSQL)"
echo "- nosql_queries_examples.md (NoSQL)"
