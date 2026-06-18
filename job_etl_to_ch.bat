@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo [1/1] starting job...
docker exec -t spark_jupyter spark-submit --packages org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.6.0-patch5 /home/jovyan/work/04_etl_to_clickhouse.py
if %errorlevel% neq 0 (
    echo.
    echo [error] etl_to_clickhouse.py failed!
    goto end
)

echo.
echo successfully finished etl_to_clickhouse.py
:end
pause