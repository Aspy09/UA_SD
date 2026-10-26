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
# aqui se crea wm.db (la bd sqlite). En railway se monta un volumen en /data
WORKDIR /data
COPY --from=build /build/target/WM_Central-1.0-SNAPSHOT.jar /app/app.jar

# configuracion por variables de entorno (en railway se cambian en Variables)
#   PUERTO_SOCKETS: puerto para los monitores
#   KAFKA_BROKER:   ip:puerto de kafka (en railway: kafka.railway.internal:19092)
#   PORT:           puerto del panel web (railway lo pone solo; si no hay, 8080)
ENV PUERTO_SOCKETS=9000
ENV KAFKA_BROKER=kafka:19092
EXPOSE 9000 8080

# sh -c para poder usar las variables; exec para que java reciba el ctrl+c y cierre bien
CMD ["sh", "-c", "exec java -jar /app/app.jar $PUERTO_SOCKETS $KAFKA_BROKER"]
