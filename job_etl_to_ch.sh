#!/bin/bash

cd "$(dirname "$0")"

echo "[1/1] starting job..."
docker exec -t spark_jupyter spark-submit --packages org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.6.0-patch5 /home/jovyan/work/04_etl_to_clickhouse.py

if [ $? -ne 0 ]; then
    echo ""
    echo "[error] etl_to_clickhouse.py failed!"
    read -p "Press [Enter] to exit..."
    exit 1
fi

echo ""
echo "successfully finished etl_to_clickhouse.py"

read -p "Press [Enter] to continue..."