#!/bin/bash

# Скрипт для автоматического запуска всех Spark jobs

echo "=========================================="
echo "Запуск ETL процессов для лабораторной работы №2"
echo "=========================================="

# Проверка, что все контейнеры запущены
echo "Проверка статуса контейнеров..."
docker-compose ps

echo ""
echo "Ожидание готовности сервисов (30 секунд)..."
sleep 30

# 1. ETL в модель звезда
echo ""
echo "=========================================="
echo "Шаг 1: ETL в модель звезда (PostgreSQL)"
echo "=========================================="
docker exec spark python /home/jovyan/work/spark_jobs/etl_to_star_schema.py

if [ $? -eq 0 ]; then
    echo "✓ ETL в модель звезда выполнен успешно"
else
    echo "✗ Ошибка при выполнении ETL в модель звезда"
    exit 1
fi

# 2. Отчеты в ClickHouse (обязательно)
echo ""
echo "=========================================="
echo "Шаг 2: Создание отчетов в ClickHouse"
echo "=========================================="
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_clickhouse.py

if [ $? -eq 0 ]; then
    echo "✓ Отчеты в ClickHouse созданы успешно"
else
    echo "✗ Ошибка при создании отчетов в ClickHouse"
fi

# 3. Отчеты в Cassandra (опционально)
echo ""
echo "=========================================="
echo "Шаг 3: Создание отчетов в Cassandra (опционально)"
echo "=========================================="
echo "Создание keyspace..."
docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS bigdata WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
sleep 5

docker exec spark python /home/jovyan/work/spark_jobs/reports_to_cassandra.py

if [ $? -eq 0 ]; then
    echo "✓ Отчеты в Cassandra созданы успешно"
else
    echo "✗ Ошибка при создании отчетов в Cassandra"
fi

# 4. Отчеты в Neo4j (опционально)
echo ""
echo "=========================================="
echo "Шаг 4: Создание отчетов в Neo4j (опционально)"
echo "=========================================="
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_neo4j.py

if [ $? -eq 0 ]; then
    echo "✓ Отчеты в Neo4j созданы успешно"
else
    echo "✗ Ошибка при создании отчетов в Neo4j"
fi

# 5. Отчеты в MongoDB (опционально)
echo ""
echo "=========================================="
echo "Шаг 5: Создание отчетов в MongoDB (опционально)"
echo "=========================================="
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_mongodb.py

if [ $? -eq 0 ]; then
    echo "✓ Отчеты в MongoDB созданы успешно"
else
    echo "✗ Ошибка при создании отчетов в MongoDB"
fi

# 6. Отчеты в Valkey (опционально)
echo ""
echo "=========================================="
echo "Шаг 6: Создание отчетов в Valkey (опционально)"
echo "=========================================="
docker exec spark python /home/jovyan/work/spark_jobs/reports_to_valkey.py

if [ $? -eq 0 ]; then
    echo "✓ Отчеты в Valkey созданы успешно"
else
    echo "✗ Ошибка при создании отчетов в Valkey"
fi

echo ""
echo "=========================================="
echo "Все ETL процессы завершены!"
echo "=========================================="
echo ""
echo "Для проверки результатов используйте команды из INSTRUCTIONS.md"
