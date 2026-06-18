@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

echo [1/2] stopping containers...
docker-compose down -v 2>nul

echo [2/2] starting containers and building images...
docker-compose up -d --build


:wait_postgres
timeout /t 3 /nobreak >nul
docker-compose ps postgres | findstr "healthy" >nul
if %errorlevel% neq 0 (
    echo waiting for PostgreSQL to be healthy...
    goto wait_postgres
)

:wait_table
timeout /t 2 /nobreak >nul
docker exec -i spark_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM mock_data;" 2>nul | findstr /R "[0-9]" >nul

if %errorlevel% neq 0 (
    echo staging...
    goto wait_table
)

echo.
echo staging done

:end
pause