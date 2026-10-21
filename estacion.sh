#!/usr/bin/env bash
# Lanza una estacion de riego: Monitor (en segundo plano) + Engine (con su menu).
# Uso:  ./estacion.sh <id_ws> "<ubicacion>"
# Por defecto usa la Central y el Kafka del docker compose local. Para usar otros (ej. Railway):
#   CENTRAL=host:puerto KAFKA=host:puerto ./estacion.sh WS-02 "Parque Sur"
set -e

if [ $# -lt 2 ]; then
    echo 'Uso: ./estacion.sh <id_ws> "<ubicacion>"'
    exit 1
fi
ID="$1"
UBICACION="$2"
CENTRAL="${CENTRAL:-central:9000}"
KAFKA="${KAFKA:-kafka:19092}"
NOMBRE="monitor-$(echo "$ID" | tr 'A-Z' 'a-z')"   # ej. monitor-ws02

# 1) Monitor en segundo plano, con nombre propio para que el engine lo encuentre
docker compose run -d --rm --no-deps --name "$NOMBRE" monitor 8001 "$CENTRAL" "$ID" "$UBICACION" > /dev/null
echo "Monitor de $ID en marcha (sus mensajes: docker logs -f $NOMBRE)"

# 2) Al salir del script (Ctrl+C o fin del engine), apagar tambien el monitor
trap 'docker rm -f "$NOMBRE" > /dev/null 2>&1' EXIT

# 3) Engine en primer plano, con su menu
docker compose run --rm --no-deps engine "$KAFKA" "$NOMBRE:8001"