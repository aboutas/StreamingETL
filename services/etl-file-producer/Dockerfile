FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/etl-file-producer-1.0.0.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]