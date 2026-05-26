#!/bin/bash

# Переходим в директорию, где находится сам скрипт (аналог cd /d "%~dp0")
cd "$(dirname "$0")" || exit 1

echo "[1/4] stopping containers..."
docker compose down 2>/dev/null

echo "[2/4] starting containers and building images..."
docker compose up -d --build

# Ожидание готовности PostgreSQL
echo "waiting for PostgreSQL to be healthy..."
while ! docker compose ps postgres 2>/dev/null | grep -q "healthy"; do
    sleep 3
done

# Ожидание наполнения таблицы mock_data (Staging)
echo "staging..."
while true; do
    # Выполняем запрос и проверяем, вернулось ли числовое значение (количество строк)
    if docker exec -i spark_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM mock_data;" 2>/dev/null | grep -qE '[0-9]'; then
        break
    fi
    sleep 2
done

echo ""
echo "staging done"
echo ""
read -n 1 -s -r -p "Press any key to start etl pipeline..."
echo -e "\n"

echo "[3/4] starting ETL pipeline..."
docker exec -t spark_jupyter spark-submit --packages org.postgresql:postgresql:42.6.0 /home/jovyan/work/03_etl_to_star.py
if [ $? -ne 0 ]; then
    echo ""
    echo "[error] etl_to_star.py failed! pipeline stopped."
    echo ""
    read -n 1 -s -r -p "Press any key to exit..."
    exit 1
fi

echo ""
echo "[4/4] starting data normalization and loading to ClickHouse..."
docker exec -t spark_jupyter spark-submit --packages org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.6.0-patch5 /home/jovyan/work/04_etl_to_clickhouse.py
if [ $? -ne 0 ]; then
    echo ""
    echo "[error] etl_to_clickhouse.py failed!"
    echo ""
    read -n 1 -s -r -p "Press any key to exit..."
    exit 1
fi

echo ""
echo "done"
echo ""

read -n 1 -s -r -p "Press any key to exit..."