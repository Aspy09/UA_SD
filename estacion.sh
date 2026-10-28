#!/usr/bin/env bash
# Lanza una estacion de riego: Monitor (en segundo plano) + Engine (con su menu).
# Uso:  ./estacion.sh <id_ws> "<ubicacion>"
# Por defecto usa la Central y el Kafka del docker compose local. Para usar otros (ej. Railway):
#   CENTRAL=host:puerto KAFKA=host:puerto ./estacion.sh WS-02 "Parque Sur"
#
# Tambien puede lanzar muchas estaciones a la vez, en segundo plano y sin menu,
# leyendo la lista de WM_WS/estaciones.txt (una por linea: ID#ubicacion):
#   CENTRAL=host:puerto KAFKA=host:puerto ./estacion.sh todas       -> todas las del fichero
#   CENTRAL=host:puerto KAFKA=host:puerto ./estacion.sh todas 10    -> solo las 10 primeras
#   ./estacion.sh parar                                             -> apaga las de segundo plano
set -e

CENTRAL="${CENTRAL:-central:9000}"
KAFKA="${KAFKA:-kafka:19092}"

# ---------- muchas estaciones a la vez ----------

# parar: borrar los monitores y engines que lanzo "todas"
if [ "$1" = "parar" ]; then
    docker rm -f $(docker ps -q --filter name=mon-ws --filter name=eng-ws) > /dev/null 2>&1 || true
    echo "Estaciones apagadas"
    exit 0
fi

if [ "$1" = "todas" ]; then
    CUANTAS="${2:-1000}"   # si no se dice cuantas, todas las del fichero
    FICHERO="$(dirname "$0")/WM_WS/estaciones.txt"

    # por cada linea: ID#ubicacion
    head -n "$CUANTAS" "$FICHERO" | while IFS='#' read -r ID UBICACION; do
        [ -z "$ID" ] && continue
        N="$(echo "$ID" | tr 'A-Z' 'a-z')"   # WS-02 -> ws-02 (nombre del contenedor)

        # monitor y su engine en segundo plano, con poca memoria porque son muchas
        docker compose run -d --rm --no-deps --name "mon-$N" -e JAVA_TOOL_OPTIONS=-Xmx32m \
            monitor 8001 "$CENTRAL" "$ID" "$UBICACION" < /dev/null > /dev/null
        docker compose run -d --rm --no-deps --name "eng-$N" -e JAVA_TOOL_OPTIONS=-Xmx64m \
            engine "$KAFKA" "mon-$N:8001" < /dev/null > /dev/null
        echo "$ID ($UBICACION) en marcha"
    done
    echo "Listo. Ver una: docker logs -f eng-ws-02   Apagarlas: ./estacion.sh parar"
    exit 0
fi

# ---------- una estacion con su menu ----------

if [ $# -lt 2 ]; then
    echo 'Uso: ./estacion.sh <id_ws> "<ubicacion>"   |   ./estacion.sh todas [n]   |   ./estacion.sh parar'
    exit 1
fi
ID="$1"
UBICACION="$2"
NOMBRE="monitor-$(echo "$ID" | tr 'A-Z' 'a-z')"   # ej. monitor-ws02

# 1) Monitor en segundo plano, con nombre propio para que el engine lo encuentre
docker compose run -d --rm --no-deps --name "$NOMBRE" monitor 8001 "$CENTRAL" "$ID" "$UBICACION" > /dev/null
echo "Monitor de $ID en marcha (sus mensajes: docker logs -f $NOMBRE)"

# 2) Al salir del script (Ctrl+C o fin del engine), apagar tambien el monitor
trap 'docker rm -f "$NOMBRE" > /dev/null 2>&1' EXIT

# 3) Engine en primer plano, con su menu
docker compose run --rm --no-deps engine "$KAFKA" "$NOMBRE:8001"
