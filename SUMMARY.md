# Итоговая сводка по лабораторной работе №2

## Что реализовано

### ✅ Обязательные компоненты (100%)

1. **Исходные данные**
   - 10 CSV файлов с mock данными (10,000 записей)
   - Автоматическая загрузка в PostgreSQL

2. **PostgreSQL - Модель звезда**
   - Таблица исходных данных: `mock_data`
   - 6 таблиц измерений:
     - `dim_customers` - клиенты
     - `dim_sellers` - продавцы
     - `dim_products` - продукты
     - `dim_stores` - магазины
     - `dim_suppliers` - поставщики
     - `dim_time` - время
   - 1 таблица фактов:
     - `fact_sales` - продажи

3. **Apache Spark ETL**
   - `etl_to_star_schema.py` - трансформация в модель звезда
   - Полная автоматизация процесса

4. **ClickHouse - 6 отчетов**
   - `product_sales_report` - витрина продаж по продуктам
   - `customer_sales_report` - витрина продаж по клиентам
   - `time_sales_report` - витрина продаж по времени
   - `store_sales_report` - витрина продаж по магазинам
   - `supplier_sales_report` - витрина продаж по поставщикам
   - `product_quality_report` - витрина качества продукции

### ✅ Опциональные компоненты (бонусы)

5. **Cassandra - 6 отчетов**
   - Все те же отчеты в формате Cassandra
   - Keyspace: `bigdata`

6. **Neo4j - 6 отчетов**
   - Граф-ориентированное представление отчетов
   - Узлы для каждого типа отчета

7. **MongoDB - 6 отчетов**
   - Документо-ориентированное хранение
   - 6 коллекций с отчетами

8. **Valkey (Redis) - 6 отчетов**
   - Key-value хранилище
   - Быстрый доступ к отчетам

## Структура проекта

```
BigDataSpark/
├── README.md                           # Основное описание
├── INSTRUCTIONS.md                     # Подробная инструкция
├── docker-compose.yml                  # Конфигурация всех сервисов
├── requirements.txt                    # Python зависимости
├── .gitignore                         # Игнорируемые файлы
│
├── исходные данные/                   # CSV файлы (10 файлов)
│   ├── MOCK_DATA.csv
│   ├── MOCK_DATA (1).csv
│   └── ...
│
├── postgres/                          # PostgreSQL конфигурация
│   └── init/
│       ├── 01_create_tables.sql      # Создание таблиц
│       └── 02_load_data.sql          # Загрузка данных
│
├── spark_jobs/                        # Spark ETL скрипты
│   ├── etl_to_star_schema.py         # ETL в звезду
│   ├── etl_to_star_schema_simple.py  # Упрощенная версия
│   ├── reports_to_clickhouse.py      # Отчеты в ClickHouse
│   ├── reports_to_cassandra.py       # Отчеты в Cassandra
│   ├── reports_to_neo4j.py           # Отчеты в Neo4j
│   ├── reports_to_mongodb.py         # Отчеты в MongoDB
│   └── reports_to_valkey.py          # Отчеты в Valkey
│
├── jars/                              # JDBC драйверы (скачиваются)
│   ├── postgresql-42.7.1.jar
│   ├── clickhouse-jdbc-0.4.6.jar
│   ├── spark-cassandra-connector_2.12-3.4.0.jar
│   ├── neo4j-connector-apache-spark_2.12-5.0.0.jar
│   ├── mongo-spark-connector_2.12-10.2.0.jar
│   └── spark-redis_2.12-3.1.0.jar
│
├── download_jars.sh                   # Скрипт скачивания JAR
├── run_all_jobs.sh                    # Запуск всех ETL
├── check_system.sh                    # Проверка системы
├── verification_queries.sql           # SQL запросы для проверки
├── nosql_queries_examples.md          # Примеры NoSQL запросов
├── clickhouse_schema.sql              # Схема ClickHouse
└── cassandra_schema.cql               # Схема Cassandra
```

## Отчеты

### 1. Витрина продаж по продуктам
**Цель:** Анализ выручки, количества продаж и популярности продуктов

**Метрики:**
- Топ-10 самых продаваемых продуктов
- Общая выручка по категориям продуктов
- Средний рейтинг и количество отзывов для каждого продукта

**Поля:**
- product_id, name, category, rating
- total_quantity, total_revenue, avg_rating, sales_count

### 2. Витрина продаж по клиентам
**Цель:** Анализ покупательского поведения и сегментация клиентов

**Метрики:**
- Топ-10 клиентов с наибольшей общей суммой покупок
- Распределение клиентов по странам
- Средний чек для каждого клиента

**Поля:**
- customer_id, first_name, last_name, country
- total_spent, purchase_count, avg_check

### 3. Витрина продаж по времени
**Цель:** Анализ сезонности и трендов продаж

**Метрики:**
- Месячные и годовые тренды продаж
- Сравнение выручки за разные периоды
- Средний размер заказа по месяцам

**Поля:**
- year, month, quarter
- total_revenue, sales_count, avg_order_size

### 4. Витрина продаж по магазинам
**Цель:** Анализ эффективности магазинов

**Метрики:**
- Топ-5 магазинов с наибольшей выручкой
- Распределение продаж по городам и странам
- Средний чек для каждого магазина

**Поля:**
- store_id, name, city, country
- total_revenue, sales_count, avg_check

### 5. Витрина продаж по поставщикам
**Цель:** Анализ эффективности поставщиков

**Метрики:**
- Топ-5 поставщиков с наибольшей выручкой
- Средняя цена товаров от каждого поставщика
- Распределение продаж по странам поставщиков

**Поля:**
- supplier_id, supplier_name, supplier_country
- total_revenue, avg_product_price, sales_count

### 6. Витрина качества продукции
**Цель:** Анализ отзывов и рейтингов товаров

**Метрики:**
- Продукты с наивысшим и наименьшим рейтингом
- Корреляция между рейтингом и объемом продаж
- Продукты с наибольшим количеством отзывов

**Поля:**
- product_id, name, rating, reviews
- total_sold, avg_rating

## Технологический стек

- **Apache Spark 3.5.0** - ETL обработка
- **PostgreSQL 13** - Реляционная БД, модель звезда
- **ClickHouse** - Колоночная OLAP БД
- **Cassandra** - Распределенная NoSQL БД
- **Neo4j 4.4** - Графовая БД
- **MongoDB** - Документо-ориентированная БД
- **Valkey (Redis Stack)** - Key-value хранилище
- **Docker & Docker Compose** - Контейнеризация
- **Python 3.x** - Язык программирования
- **PySpark** - Python API для Spark

## Как запустить

### Быстрый старт (3 команды)
```bash
./download_jars.sh          # Скачать JAR файлы
docker-compose up -d        # Запустить инфраструктуру
./run_all_jobs.sh          # Запустить все ETL процессы
```

### Проверка результатов
```bash
./check_system.sh          # Проверить все компоненты
```

## Доступ к сервисам

| Сервис | URL/Порт | Логин | Пароль |
|--------|----------|-------|--------|
| PostgreSQL | localhost:5432 | user | password |
| Spark Jupyter | http://localhost:8888 | - | см. логи |
| ClickHouse | localhost:8123, 9000 | - | - |
| Cassandra | localhost:9042 | - | - |
| Neo4j | http://localhost:7474 | neo4j | password |
| MongoDB | localhost:27017 | - | - |
| Valkey | localhost:6379 | - | - |

## Результаты

✅ **Все требования выполнены:**
- 10,000 записей исходных данных загружены
- Модель звезда создана в PostgreSQL
- 6 отчетов созданы в ClickHouse (обязательно)
- 6 отчетов созданы в Cassandra (бонус)
- 6 отчетов созданы в Neo4j (бонус)
- 6 отчетов созданы в MongoDB (бонус)
- 6 отчетов созданы в Valkey (бонус)

✅ **Дополнительно:**
- Полная автоматизация через скрипты
- Подробная документация
- Примеры запросов для всех БД
- Скрипты проверки системы

## Автор
Лабораторная работа №2 по курсу "Анализ больших данных"

## Лицензия
Учебный проект
