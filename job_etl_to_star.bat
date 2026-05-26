@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo [1/1] starting job...
docker exec -t spark_jupyter spark-submit --packages org.postgresql:postgresql:42.6.0 /home/jovyan/work/03_etl_to_star.py
if %errorlevel% neq 0 (
    echo.
    echo [error] etl_to_star.py failed! pipeline stopped.
    goto end
)

echo.
echo successfully finished etl_to_star.py
:end
pause