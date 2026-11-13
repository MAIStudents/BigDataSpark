FROM apache/spark:3.5.3-java17-python3

USER root

ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar
ADD --chmod=644 https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar /opt/spark/jars/clickhouse-jdbc-0.4.6-all.jar

USER spark
