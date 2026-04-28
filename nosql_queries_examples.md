# Примеры запросов для NoSQL баз данных

## ClickHouse

```sql
-- Подключение
docker exec -it clickhouse clickhouse-client

-- Просмотр всех таблиц
SHOW TABLES;

-- Топ-10 продуктов по выручке
SELECT 
    name,
    category,
    total_revenue,
    total_quantity
FROM product_sales_report
ORDER BY total_revenue DESC
LIMIT 10;

-- Топ-10 клиентов
SELECT 
    first_name,
    last_name,
    country,
    total_spent
FROM customer_sales_report
ORDER BY total_spent DESC
LIMIT 10;

-- Продажи по месяцам
SELECT 
    year,
    month,
    total_revenue,
    sales_count
FROM time_sales_report
ORDER BY year, month;

-- Топ-5 магазинов
SELECT 
    name,
    city,
    country,
    total_revenue
FROM store_sales_report
ORDER BY total_revenue DESC
LIMIT 5;

-- Топ-5 поставщиков
SELECT 
    supplier_name,
    supplier_country,
    total_revenue
FROM supplier_sales_report
ORDER BY total_revenue DESC
LIMIT 5;

-- Продукты с наивысшим рейтингом
SELECT 
    name,
    rating,
    total_sold
FROM product_quality_report
ORDER BY rating DESC, total_sold DESC
LIMIT 10;
```

## Cassandra

```cql
-- Подключение
docker exec -it cassandra cqlsh

-- Использование keyspace
USE bigdata;

-- Просмотр таблиц
DESCRIBE TABLES;

-- Просмотр данных из таблиц
SELECT * FROM product_sales_report LIMIT 10;
SELECT * FROM customer_sales_report LIMIT 10;
SELECT * FROM time_sales_report LIMIT 10;
SELECT * FROM store_sales_report LIMIT 10;
SELECT * FROM supplier_sales_report LIMIT 10;
SELECT * FROM product_quality_report LIMIT 10;

-- Поиск по конкретному продукту
SELECT * FROM product_sales_report WHERE product_id = 1;

-- Поиск по клиенту
SELECT * FROM customer_sales_report WHERE customer_id = 1;
```

## Neo4j

```cypher
-- Подключение через браузер: http://localhost:7474
-- Логин: neo4j, Пароль: password

-- Просмотр всех узлов ProductSalesReport
MATCH (n:ProductSalesReport)
RETURN n
LIMIT 10;

-- Топ-10 продуктов по выручке
MATCH (n:ProductSalesReport)
RETURN n.name, n.category, n.total_revenue
ORDER BY n.total_revenue DESC
LIMIT 10;

-- Просмотр всех узлов CustomerSalesReport
MATCH (n:CustomerSalesReport)
RETURN n
LIMIT 10;

-- Топ-10 клиентов
MATCH (n:CustomerSalesReport)
RETURN n.first_name, n.last_name, n.country, n.total_spent
ORDER BY n.total_spent DESC
LIMIT 10;

-- Продажи по времени
MATCH (n:TimeSalesReport)
RETURN n.year, n.month, n.total_revenue
ORDER BY n.year, n.month;

-- Топ-5 магазинов
MATCH (n:StoreSalesReport)
RETURN n.name, n.city, n.total_revenue
ORDER BY n.total_revenue DESC
LIMIT 5;

-- Топ-5 поставщиков
MATCH (n:SupplierSalesReport)
RETURN n.supplier_name, n.supplier_country, n.total_revenue
ORDER BY n.total_revenue DESC
LIMIT 5;

-- Продукты с высоким рейтингом
MATCH (n:ProductQualityReport)
WHERE n.rating > 4.0
RETURN n.name, n.rating, n.total_sold
ORDER BY n.rating DESC
LIMIT 10;

-- Подсчет всех узлов каждого типа
MATCH (n:ProductSalesReport) RETURN count(n) as ProductSalesCount
UNION
MATCH (n:CustomerSalesReport) RETURN count(n) as CustomerSalesCount
UNION
MATCH (n:TimeSalesReport) RETURN count(n) as TimeSalesCount
UNION
MATCH (n:StoreSalesReport) RETURN count(n) as StoreSalesCount
UNION
MATCH (n:SupplierSalesReport) RETURN count(n) as SupplierSalesCount
UNION
MATCH (n:ProductQualityReport) RETURN count(n) as ProductQualityCount;
```

## MongoDB

```javascript
// Подключение
docker exec -it mongodb mongosh

// Использование базы данных
use bigdata

// Просмотр коллекций
show collections

// Топ-10 продуктов по выручке
db.product_sales_report.find().sort({total_revenue: -1}).limit(10).pretty()

// Топ-10 клиентов
db.customer_sales_report.find().sort({total_spent: -1}).limit(10).pretty()

// Продажи по времени
db.time_sales_report.find().sort({year: 1, month: 1}).pretty()

// Топ-5 магазинов
db.store_sales_report.find().sort({total_revenue: -1}).limit(5).pretty()

// Топ-5 поставщиков
db.supplier_sales_report.find().sort({total_revenue: -1}).limit(5).pretty()

// Продукты с рейтингом выше 4.0
db.product_quality_report.find({rating: {$gt: 4.0}}).sort({rating: -1}).limit(10).pretty()

// Подсчет документов в каждой коллекции
db.product_sales_report.countDocuments()
db.customer_sales_report.countDocuments()
db.time_sales_report.countDocuments()
db.store_sales_report.countDocuments()
db.supplier_sales_report.countDocuments()
db.product_quality_report.countDocuments()

// Агрегация: средняя выручка по категориям
db.product_sales_report.aggregate([
    {$group: {
        _id: "$category",
        avg_revenue: {$avg: "$total_revenue"},
        total_products: {$sum: 1}
    }},
    {$sort: {avg_revenue: -1}}
])

// Агрегация: распределение клиентов по странам
db.customer_sales_report.aggregate([
    {$group: {
        _id: "$country",
        customer_count: {$sum: 1},
        total_revenue: {$sum: "$total_spent"}
    }},
    {$sort: {total_revenue: -1}}
])
```

## Valkey (Redis)

```bash
# Подключение
docker exec -it valkey redis-cli

# Просмотр всех ключей
KEYS *

# Просмотр ключей по паттерну
KEYS product_sales_report:*
KEYS customer_sales_report:*

# Получение данных по конкретному ключу
GET product_sales_report:1
GET customer_sales_report:1

# Получение всех ключей для каждого отчета
KEYS product_sales_report:*
KEYS customer_sales_report:*
KEYS time_sales_report:*
KEYS store_sales_report:*
KEYS supplier_sales_report:*
KEYS product_quality_report:*

# Подсчет количества ключей
KEYS product_sales_report:* | wc -l

# Получение типа данных
TYPE product_sales_report:1

# Получение информации о базе данных
INFO keyspace

# Получение всех полей hash (если используется hash)
HGETALL product_sales_report:1

# Поиск по паттерну и вывод первых 10
SCAN 0 MATCH product_sales_report:* COUNT 10
```

## Сравнительный анализ

### Производительность запросов

```bash
# ClickHouse - самый быстрый для аналитических запросов
time docker exec -it clickhouse clickhouse-client --query "SELECT COUNT(*) FROM product_sales_report"

# PostgreSQL - хорош для транзакционных запросов
time docker exec -it postgres psql -U user -d bigdata -c "SELECT COUNT(*) FROM fact_sales"

# MongoDB - гибкий для документо-ориентированных запросов
time docker exec -it mongodb mongosh --eval "db.product_sales_report.countDocuments()"
```

### Размер данных

```bash
# PostgreSQL
docker exec -it postgres psql -U user -d bigdata -c "SELECT pg_size_pretty(pg_database_size('bigdata'))"

# ClickHouse
docker exec -it clickhouse clickhouse-client --query "SELECT formatReadableSize(sum(bytes)) FROM system.parts WHERE database = 'default'"

# MongoDB
docker exec -it mongodb mongosh --eval "db.stats()"

# Cassandra
docker exec -it cassandra nodetool status
```
