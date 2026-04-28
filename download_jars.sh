#!/bin/bash

# Скрипт для скачивания всех необходимых JAR файлов

echo "Создание директории jars..."
mkdir -p jars

echo "Скачивание PostgreSQL JDBC Driver..."
wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O jars/postgresql-42.7.1.jar

echo "Скачивание ClickHouse JDBC Driver..."
wget -q https://github.com/ClickHouse/clickhouse-java/releases/download/v0.4.6/clickhouse-jdbc-0.4.6-all.jar -O jars/clickhouse-jdbc-0.4.6.jar

echo "Скачивание Cassandra Spark Connector..."
wget -q https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.4.0/spark-cassandra-connector_2.12-3.4.0.jar -O jars/spark-cassandra-connector_2.12-3.4.0.jar

echo "Скачивание Neo4j Spark Connector..."
wget -q https://github.com/neo4j-contrib/neo4j-spark-connector/releases/download/5.0.0/neo4j-connector-apache-spark_2.12-5.0.0.jar -O jars/neo4j-connector-apache-spark_2.12-5.0.0.jar

echo "Скачивание MongoDB Spark Connector..."
wget -q https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.2.0/mongo-spark-connector_2.12-10.2.0.jar -O jars/mongo-spark-connector_2.12-10.2.0.jar

echo "Скачивание Redis Spark Connector..."
wget -q https://repo1.maven.org/maven2/com/redislabs/spark-redis_2.12/3.1.0/spark-redis_2.12-3.1.0.jar -O jars/spark-redis_2.12-3.1.0.jar

echo "Все JAR файлы успешно скачаны!"
ls -lh jars/
