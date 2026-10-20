# --- Etapa 1: compilar con Maven ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q dependency:go-offline     # descarga librerias (se reaprovecha si el pom no cambia)
COPY src ./src
RUN mvn -q package -DskipTests

#Etapa 2
FROM eclipse-temurin:17-jre
WORKDIR /data                        # aqui se crea wm.db (la bd sqlite)
COPY --from=build /build/target/WM_Central-1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
CMD ["9000", "kafka:19092"]          # argumentos por defecto (se pueden cambiar)