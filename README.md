# Анализ больших данных — Лабораторная работа №2
**Тема:** ETL реализованный с помощью Spark.

В данном проекте реализован ETL-пайплайн с использованием Apache Spark. Пайплайн загружает сырые данные из CSV, преобразует их в схему «Звезда» в PostgreSQL, а затем на её основе строит 3 аналитических отчета.

## Структура проекта
* `data/` — исходные файлы `MOCK_DATA_*.csv` (10 файлов по 1000 строк).
* `jars/` — JDBC драйверы для PostgreSQL и ClickHouse.
* `docker-compose.yml` — конфигурация инфраструктуры (PostgreSQL, ClickHouse, Apache Spark).
* `job1_etl_postgres.py` — PySpark-скрипт трансформации сырых CSV в PostgreSQL (модель «Звезда»).
* `job2_etl_clickhouse.py` — PySpark-скрипт генерации 3 аналитических отчетов.
* `postgre.png` — скриншот результатов в PostgreSQL.
* `click_house_1.png` — скриншот сгенерированных отчетов.

## Инструкция по запуску

### 1. Поднятие инфраструктуры
Запустите базы данных и контейнер со Spark:
```bash
docker-compose up -d
```
### 2. Запуск ETL (Job 1)
Трансформация CSV данных в звездообразную модель PostgreSQL:

```bash
docker exec -it spark_master spark-submit --jars /home/jovyan/work/jars/postgresql-42.7.1.jar /home/jovyan/work/job1_etl_postgres.py
```

### 3. Генерация отчетов (Job 2)
Формирование аналитических отчетов на основе звездообразной модели:

```bash
docker exec -it spark_master spark-submit --jars /home/jovyan/work/jars/postgresql-42.7.1.jar /home/jovyan/work/job2_etl_clickhouse.py
```

### 4. Проверка результатов в PostgreSQL
Подключитесь к PostgreSQL через DBeaver:

```bash
Host: localhost:5432

Database: petshop_db

User: admin

Password: adminpassword
```

### Выполните проверочный запрос:

```sql
SELECT 'fact_sales' as table_name, COUNT(*) as records FROM fact_sales
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store;
```

В репозитории представлены скриншоты работы:

postgre.png — результат выполнения запросов в PostgreSQL (количество записей в таблицах).

clickhouse.png — сгенерированные отчеты в консоли Spark.