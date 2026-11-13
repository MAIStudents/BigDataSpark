# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark.

Выполнил: Крючков Артемий Владимирович, группа М8О-205СВ-25.

## Как запустить лабораторную работу

Требуется установленный Docker (при первой сборке нужен интернет: качаются образы и JDBC-драйверы с Maven Central).

Из корня репозитория:

```bash
docker compose up -d --build
```

При старте автоматически выполняется:

1. Поднимается PostgreSQL 17 (порт на хосте 5433). Создаётся таблица `mock_data`, через `COPY` импортируются все 10 файлов `исходные данные/MOCK_DATA(*).csv` (10000 строк), затем выполняется DDL модели звезда.
2. Поднимается ClickHouse 24.8 (порт на хосте 8123), создаётся база `reports`.
3. После готовности обеих БД запускается контейнер Spark (PySpark 3.5, `spark-submit --master local[4]`) и выполняет две джобы:
   - `jobs/01_mock_to_star.py` — читает `mock_data` из PostgreSQL, строит измерения и факты, пишет модель звезда обратно в PostgreSQL;
   - `jobs/02_star_to_reports.py` — читает звезду из PostgreSQL и создаёт 6 отчётов-витрин в ClickHouse (таблицы MergeTree в базе `reports`).

Готовность джоб можно проверить логами (контейнер `bigdataspark_etl` должен завершиться со статусом Exited (0)):

```bash
docker compose ps -a
docker logs bigdataspark_etl
```

Повторный запуск ETL без пересоздания баз:

```bash
docker compose run --rm spark
```

## Подключение (DBeaver)

PostgreSQL:

| Параметр | Значение     |
| -------- | ------------ |
| Host     | localhost    |
| Port     | 5433         |
| Database | bigdataspark |
| User     | postgres     |
| Password | postgres     |

ClickHouse:

| Параметр | Значение   |
| -------- | ---------- |
| Host     | localhost  |
| Port     | 8123       |
| Database | reports    |
| User     | default    |
| Password | clickhouse |

## Проверка результата

Модель звезда в PostgreSQL: `fact_sales` + `dim_date`, `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`. Суррогатные ключи назначает Spark (`row_number`); исходные `id`/`sale_*_id` в данных не уникальны, поэтому измерения строятся по полным наборам атрибутов.

Контрольные значения:

```sql
SELECT count(*) FROM mock_data;
SELECT count(*) FROM fact_sales;
SELECT sum(total_price) FROM fact_sales;
```

Ожидается: 10000, 10000, 2529852.12.

Витрины в ClickHouse:

| Отчёт                  | Таблица                       | Строк |
| ---------------------- | ----------------------------- | ----: |
| Витрина по продуктам  | reports.rpt_product_sales    |  3233 |
| Витрина по клиентам   | reports.rpt_customer_sales   | 10000 |
| Витрина по времени    | reports.rpt_time_sales       |    12 |
| Витрина по магазинам  | reports.rpt_store_sales      | 10000 |
| Витрина по поставщикам | reports.rpt_supplier_sales  | 10000 |
| Витрина качества      | reports.rpt_product_quality  |  3233 |

Примеры запросов по витринам:

Топ-10 самых продаваемых продуктов:

```sql
SELECT product_name, product_brand, sales_cnt, total_revenue
FROM reports.rpt_product_sales
ORDER BY sales_cnt DESC, total_revenue DESC
LIMIT 10;
```

Выручка по категориям продуктов:

```sql
SELECT product_category, sum(total_revenue) AS revenue, sum(sales_cnt) AS sales_cnt
FROM reports.rpt_product_sales
GROUP BY product_category
ORDER BY revenue DESC;
```

Топ-10 клиентов с наибольшей суммой покупок:

```sql
SELECT first_name, last_name, email, country, total_revenue
FROM reports.rpt_customer_sales
ORDER BY total_revenue DESC
LIMIT 10;
```

Распределение клиентов по странам:

```sql
SELECT country, count(*) AS customers
FROM reports.rpt_customer_sales
GROUP BY country
ORDER BY customers DESC;
```

Месячные тренды продаж:

```sql
SELECT year, month, month_name, sales_cnt, total_revenue
FROM reports.rpt_time_sales
ORDER BY year, month;
```

Топ-5 магазинов с наибольшей выручкой:

```sql
SELECT store_name, store_city, store_country, total_revenue
FROM reports.rpt_store_sales
ORDER BY total_revenue DESC
LIMIT 5;
```

Топ-5 поставщиков с наибольшей выручкой:

```sql
SELECT supplier_name, supplier_country, total_revenue, avg_product_price
FROM reports.rpt_supplier_sales
ORDER BY total_revenue DESC
LIMIT 5;
```

Корреляция между рейтингом и объёмом продаж:

```sql
SELECT corr(avg_rating, sales_cnt) AS correlation
FROM reports.rpt_product_quality;
```

Продукты с наивысшим и наименьшим рейтингом:

```sql
SELECT * FROM reports.rpt_product_quality ORDER BY avg_rating DESC, avg_reviews DESC LIMIT 10;
SELECT * FROM reports.rpt_product_quality ORDER BY avg_rating ASC, avg_reviews DESC LIMIT 10;
```

## Управление

```bash
docker compose stop
docker compose down
docker compose down -v && docker compose up -d --build
```

`down -v` удаляет данные и запускает полную переинициализацию с нуля.

## Состав решения

| Файл                         | Назначение                                                          |
| ---------------------------- | ------------------------------------------------------------------- |
| `docker-compose.yml`         | PostgreSQL 17, ClickHouse 24.8, контейнер Spark с автозапуском ETL   |
| `Dockerfile`                 | Образ Spark 3.5 (PySpark) с JDBC-драйверами PostgreSQL и ClickHouse |
| `init/01_mock_data.sql`      | таблица-источник `mock_data` и импорт 10 CSV                        |
| `init/02_ddl.sql`            | DDL модели звезда в PostgreSQL                                      |
| `jobs/01_mock_to_star.py`    | Spark-джоба: mock_data → звезда в PostgreSQL                        |
| `jobs/02_star_to_reports.py` | Spark-джоба: звезда → 6 отчётов в ClickHouse                        |

---

## Исходное задание

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Одним из самых популярных фреймворков для работы с Big Data является Apache Spark. Apache Spark - мощный фреймворк, который предлагает широкий набор функциональности для простого написания ETL-пайплайнов.

Что необходимо сделать? 

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv с номерами) в модель данных звезда в PostgreSQL, а затем на основе модели данных звезда создать ряд отчетов по данным в одной из NoSQL базах данных обязательно и в нескольких других опционально (будет бонусом). Каждый отчет представляет собой отдельную таблицу в NoSQL БД.

Какие отчеты надо создать?
1. Витрина продаж по продуктам
Цель: Анализ выручки, количества продаж и популярности продуктов.
 - Топ-10 самых продаваемых продуктов.
 - Общая выручка по категориям продуктов.
 - Средний рейтинг и количество отзывов для каждого продукта.
2. Витрина продаж по клиентам
Цель: Анализ покупательского поведения и сегментация клиентов.
 - Топ-10 клиентов с наибольшей общей суммой покупок.
 - Распределение клиентов по странам.
 - Средний чек для каждого клиента.
3. Витрина продаж по времени
Цель: Анализ сезонности и трендов продаж.
 - Месячные и годовые тренды продаж.
 - Сравнение выручки за разные периоды.
 - Средний размер заказа по месяцам.
4. Витрина продаж по магазинам
Цель: Анализ эффективности магазинов.
 - Топ-5 магазинов с наибольшей выручкой.
 - Распределение продаж по городам и странам.
 - Средний чек для каждого магазина.
5. Витрина продаж по поставщикам
Цель: Анализ эффективности поставщиков.
 - Топ-5 поставщиков с наибольшей выручкой.
 - Средняя цена товаров от каждого поставщика.
 - Распределение продаж по странам поставщиков.
6. Витрина качества продукции
Цель: Анализ отзывов и рейтингов товаров.
 - Продукты с наивысшим и наименьшим рейтингом.
 - Корреляция между рейтингом и объемом продаж.
 - Продукты с наибольшим количеством отзывов.

В каких NoSQL БД должны быть эти отчеты:
1. **Clickhouse** **(обязательно)**
2. Cassandra (опционально, если будет реализация, то это бонус)
3. Neo4J (опционально, если будет реализация, то это бонус)
4. MongoDB (опционально, если будет реализация, то это бонус)
5. Valkey (опционально, если будет реализация, то это бонус)

![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете себе инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Spark (рекомендую установку через Docker. Для удобства написания кода на Python можно запустить вместе со JupyterNotebook. Для Java - подключить volume и собрать образ Docker, который будет запускать команду spark-submit с java jar-файлом при старте контейнера, сам jar файл собирается отдельно и кладется в подключенный volume)
5. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
6. Импортируете данные в БД PostgreSQL (например, через механизм импорта csv в DBeaver). Всего в таблице mock_data должно находиться 10000 строк из 10 файлов.
7. Анализируете исходные данные с помощью запросов.
8. Выявляете сущности фактов и измерений.
9. Реализуете приложение на Spark, которое по аналогии с первой лабораторной работой перекладывает исходные данные из PostgreSQL в модель снежинку/звезда в PostgreSQL. (Убедитесь в коннективности Spark и PostgreSQL, настройте сеть между Spark и PostgreSQL, если используете Docker).
10. Устанавливаете ClickHouse (рекомендую установку через Docker. Убедитесь в коннективности Spark и Clickhouse, настройте сеть между Spark и ClickHouse). **(обязательно)**
11. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в ClickHouse. **(обязательно)**
12. Устанавливаете Cassandra (рекомендую установку через Docker. Убедитесь в коннективности Spark и Cassandra, настройте сеть между Spark и Cassandra). (опционально)
13. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в Cassandra. (опционально)
14. Устанавливаете Neo4j (рекомендую установку через Docker. Убедитесь в коннективности Spark и Neo4j, настройте сеть между Spark и Neo4j). (опционально)
15. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных сущностей в Neo4j. (опционально)
16. Устанавливаете MongoDB (рекомендую установку через Docker. Убедитесь в коннективности Spark и MongoDB, настройте сеть между Spark и MongoDB). (опционально)
17. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных коллекций в MongoDB. (опционально)
18. Устанавливаете Valkey (рекомендую установку через Docker. Убедитесь в коннективности Spark и Valkey, настройте сеть между Spark и Valkey). (опционально)
19. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных записей в Valkey. (опционально)
20. Проверяете отчеты в каждой базе данных средствами языка самой БД (ClickHouse - SQL (DBeaver), Cassandra - CQL (DBeaver), Neo4J - Cipher (DBeaver), MongoDB - MQL (Compass), Valkey - redis-cli).
21. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Spark, ClickHouse **(обязательно)**, Cassandra (опционально), Neo4j (опционально), MongoDB (опционально), Valkey (опционально) и заполненными данными в PostgreSQL из файлов mock_data(*).csv.
3. Инструкция, как запускать Spark-джобы для проверки лабораторной работы.
4. Код Apache Spark трансформации данных из исходной модели в снежинку/звезду в PostgreSQL.
5. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в ClickHouse.
6. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Cassandra.
7. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Neo4j.
8. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в MongoDB.
9. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Valkey.
