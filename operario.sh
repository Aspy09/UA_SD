#!/usr/bin/env bash
# Lanza un operario con su menu. Con un fichero, lo ejecuta al arrancar.
# Uso:  ./operario.sh <id_operario> [fichero]
# Para usar otro Kafka (ej. Railway):  KAFKA=host:puerto ./operario.sh FO-01
set -e

if [ $# -lt 1 ]; then
    echo 'Uso: ./operario.sh <id_operario> [fichero]'
    exit 1
fi
KAFKA="${KAFKA:-kafka:19092}"

if [ -n "$2" ]; then
    docker compose run --rm --no-deps -v "$(realpath "$2"):/app/peticiones.txt:ro" fo "$KAFKA" "$1" peticiones.txt
else
    docker compose run --rm --no-deps fo "$KAFKA" "$1"
fi