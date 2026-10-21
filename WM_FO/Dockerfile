# --- Etapa 1: compilar con Maven ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
# descarga librerias (se reaprovecha si el pom no cambia)
RUN mvn -q dependency:go-offline     
COPY src ./src
RUN mvn -q package -DskipTests

# --- Etapa 2: imagen final, solo con Java para ejecutar ---
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/WM_FO-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]