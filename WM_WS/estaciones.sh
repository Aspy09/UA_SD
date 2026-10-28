#!/usr/bin/env bash
# Lanza muchas estaciones a la vez (Monitor + Engine de cada una) en segundo plano,
# leyendo la lista de WM_WS/estaciones.txt (una por linea: ID#ubicacion)
#
# Uso (desde cualquier carpeta del repo):
#   CENTRAL=host:puerto KAFKA=host:puerto ./WM_WS/estaciones.sh        -> lanza todas
#   CENTRAL=host:puerto KAFKA=host:puerto ./WM_WS/estaciones.sh 10     -> lanza solo las 10 primeras
#   ./WM_WS/estaciones.sh parar                                        -> apaga todas
#
# Sin CENTRAL y KAFKA usa las del docker compose local (central:9000 y kafka:19092)

# docker compose se tiene que ejecutar en la raiz del repo (donde esta docker-compose.yml)
cd "$(dirname "$0")/.." || exit 1

# parar: borrar todos los contenedores de monitores y engines que lanzo este script
if [ "$1" = "parar" ]; then
    docker rm -f $(docker ps -q --filter name=mon-ws --filter name=eng-ws) > /dev/null 2>&1
    echo "Estaciones apagadas"
    exit 0
fi

CENTRAL="${CENTRAL:-central:9000}"
KAFKA="${KAFKA:-kafka:19092}"
CUANTAS="${1:-1000}"   # si no se dice cuantas, todas las del fichero

# por cada linea: ID#ubicacion (head -n para lanzar solo las primeras)
head -n "$CUANTAS" WM_WS/estaciones.txt | while IFS='#' read -r ID UBI; do
    [ -z "$ID" ] && continue
    NOMBRE=$(echo "$ID" | tr 'A-Z' 'a-z')   # WS-02 -> ws-02 (nombre del contenedor)

    # 1) el monitor (se registra en la central). Poca memoria porque son muchas
    docker compose run -d --rm --no-deps --name "mon-$NOMBRE" -e JAVA_TOOL_OPTIONS=-Xmx32m \
        monitor 8001 "$CENTRAL" "$ID" "$UBI" < /dev/null > /dev/null

    # 2) su engine, conectado a su monitor por el nombre del contenedor
    docker compose run -d --rm --no-deps --name "eng-$NOMBRE" -e JAVA_TOOL_OPTIONS=-Xmx64m \
        engine "$KAFKA" "mon-$NOMBRE:8001" < /dev/null > /dev/null

    echo "$ID ($UBI) en marcha"
done

echo "Listo. Para ver una:  docker logs -f eng-ws-02    Para apagarlas:  ./WM_WS/estaciones.sh parar"
