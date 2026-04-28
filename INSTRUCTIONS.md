# Инструкция по запуску

## Требования
- Docker и Docker Compose
- 8GB RAM минимум
- 10GB свободного места

## Шаг 1: Скачать JAR файлы

Скачайте необходимые JDBC драйверы:

```bash
./download_jars.sh
```

Или вручную:
- PostgreSQL JDBC: https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
- ClickHouse JDBC: https://github.com/ClickHouse/clickhouse-java/releases
- Cassandra Connector: https://repo1.maven.org/maven2/com/datastax/spark/
- Neo4j Connector: https://github.com/neo4j-contrib/neo4j-spark-connector/releases
- MongoDB Connector: https://repo1.maven.org/maven2/org/mongodb/spark/
- Redis Connector: https://repo1.maven.org/maven2/com/redislabs/

## Шаг 2: Запуск инфраструктуры

```bash
docker-compose up -d
```

Подождите 2-3 минуты пока все контейнеры запустятся.

## Шаг 3: Проверка PostgreSQL

```bash
docker exec -it postgres psql -U user -d bigdata -c "SELECT COUNT(*) FROM mock_data;"
```

Должно быть 10000 записей.

## Шаг 4: Запуск ETL в модель звезда

```bash
docker exec spark python /home/jovyan/work/spark_jobs/etl_to_star_schema.py
```

## Шаг 5: Создание отчетов в ClickHouse

```bash
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_clickhouse.py
```

## Шаг 6: Создание отчетов в Cassandra (опционально)

Создайте keyspace:
```bash
docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS bigdata WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
```

Запустите job:
```bash
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_cassandra.py
```

## Шаг 7: Создание отчетов в Neo4j (опционально)

```bash
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_neo4j.py
```

## Шаг 8: Создание отчетов в MongoDB (опционально)

```bash
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_mongodb.py
```

## Шаг 9: Создание отчетов в Valkey (опционально)

```bash
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_valkey.py
```

## Автоматический запуск всех jobs

```bash
./run_all_jobs.sh
```

## Проверка результатов

### PostgreSQL
```bash
docker exec -it postgres psql -U user -d bigdata
```

```sql
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM fact_sales;
```

### ClickHouse
```bash
docker exec -it clickhouse clickhouse-client
```

```sql
SHOW TABLES;
SELECT * FROM product_sales_report LIMIT 10;
```

### Cassandra
```bash
docker exec -it cassandra cqlsh
```

```cql
USE bigdata;
DESCRIBE TABLES;
SELECT * FROM product_sales_report LIMIT 10;
```

### Neo4j
Откройте http://localhost:7474
Логин: neo4j, Пароль: password

```cypher
MATCH (n:ProductSalesReport) RETURN n LIMIT 10;
```

### MongoDB
```bash
docker exec -it mongodb mongosh
```

```javascript
use bigdata
db.product_sales_report.find().limit(10)
```

### Valkey
```bash
docker exec -it valkey redis-cli
```

```
KEYS *
GET product_sales_report:1
```

## Остановка

```bash
docker-compose down
```

Для удаления данных:
```bash
docker-compose down -v
```

## Troubleshooting

### Контейнеры не запускаются
```bash
docker-compose logs [service_name]
```

### Ошибки подключения
```bash
docker-compose restart
```

### Проверка сети
```bash
docker network inspect bigdataspark_bigdata-net
```
