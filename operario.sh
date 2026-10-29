#!/usr/bin/env bash
# Lanza operarios (FO) con docker. Los operarios que existen estan en
# WM_Central/src/main/resources/operarios.txt (FO-01#nombre#provincia), los da de alta la central.
#
# Uso:
#   ./operario.sh FO-05                      -> un operario con su menu en esta terminal
#   ./operario.sh FO-05 fichero.txt          -> igual pero ejecuta las peticiones del fichero al arrancar
#   ./operario.sh lista                      -> ver los operarios que hay
#   ./operario.sh todos fichero.txt          -> todos los operarios a la vez en segundo plano con el fichero
#   ./operario.sh todos fichero.txt 5        -> solo los 5 primeros
#   ./operario.sh parar                      -> apagar los de segundo plano
#
# Para usar otro Kafka (ej. Railway):  KAFKA=host:puerto ./operario.sh FO-01

KAFKA="${KAFKA:-kafka:19092}"
cd "$(dirname "$0")" || exit 1          # docker compose se ejecuta en la raiz del repo
FICHERO="WM_Central/src/main/resources/operarios.txt"

# nombre del contenedor: FO-05 -> op-fo-05
nombre() { echo "$1" | tr 'A-Z' 'a-z'; }

# provincia de un operario segun el fichero (asi solo ve las estaciones de su provincia)
region() { grep -m1 "^$1#" "$FICHERO" | cut -d'#' -f3; }

# --progress quiet: que no salgan los mensajes de "Container ... Creating/Created"
dc() { docker compose --progress quiet "$@"; }

case "$1" in

    # ---------- lista de operarios ----------
    lista)
        echo "Operarios dados de alta en la central:"
        while IFS='#' read -r ID NOMBRE REG; do
            [ -n "$ID" ] && echo "  $ID  $NOMBRE ($REG)"
        done < "$FICHERO"
        ;;

    # ---------- todos a la vez en segundo plano, cada uno con el fichero de peticiones ----------
    todos)
        [ -z "$2" ] && { echo "Uso: ./operario.sh todos fichero.txt [n]"; exit 1; }
        PETICIONES="$(realpath "$2")"
        head -n "${3:-1000}" "$FICHERO" | while IFS='#' read -r ID NOMBRE REG; do
            [ -z "$ID" ] && continue
            dc run -d --rm --no-deps --name "op-$(nombre "$ID")" -e JAVA_TOOL_OPTIONS=-Xmx64m -e REGION="$REG" \
                -v "$PETICIONES:/app/peticiones.txt:ro" fo "$KAFKA" "$ID" peticiones.txt < /dev/null > /dev/null
            echo "$ID ($NOMBRE, $REG) en marcha con $2"
        done
        echo "Listo. Ver uno: docker logs -f op-fo-01   Apagarlos: ./operario.sh parar"
        ;;

    # ---------- apagar los de segundo plano ----------
    parar)
        docker rm -f $(docker ps -q --filter name=op-fo) > /dev/null 2>&1
        echo "Operarios apagados"
        ;;

    # ---------- un operario con su menu ----------
    "")
        echo "Uso: ./operario.sh FO-05 [fichero] | lista | todos fichero [n] | parar"
        exit 1
        ;;
    *)
        # avisar si el id no esta en la lista (la central le denegara los riegos)
        grep -q "^$1#" "$FICHERO" || echo "Ojo: $1 no esta en $FICHERO, la central no lo conoce. Mira ./operario.sh lista"
        if [ -n "$2" ]; then
            dc run --rm --no-deps -e REGION="$(region "$1")" -v "$(realpath "$2"):/app/peticiones.txt:ro" fo "$KAFKA" "$1" peticiones.txt
        else
            dc run --rm --no-deps -e REGION="$(region "$1")" fo "$KAFKA" "$1"
        fi
        ;;
esac
