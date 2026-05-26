#!/bin/bash

cd "$(dirname "$0")"

echo "[1/1] starting job..."
docker exec -t spark_jupyter spark-submit --packages org.postgresql:postgresql:42.6.0 /home/jovyan/work/03_etl_to_star.py

if [ $? -ne 0 ]; then
    echo ""
    echo "[error] etl_to_star.py failed! pipeline stopped."
    read -p "Press [Enter] to exit..."
    exit 1
fi

echo ""
echo "successfully finished etl_to_star.py"

read -p "Press [Enter] to continue..."