# Spring Boot 4 + Spark (локальный Spark в JVM приложения).
FROM eclipse-temurin:21-jdk-jammy AS builder
RUN apt-get update && apt-get install -y --no-install-recommends maven \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -e package -DskipTests

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=builder /build/target/bigdataspark-*.jar /app/app.jar
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-Xms256m -Xmx1536m"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
