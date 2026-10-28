#!/usr/bin/env bash
# Lanza estaciones de riego (Monitor + Engine de cada una) con docker.
# La ubicacion de cada estacion se coge de WM_WS/estaciones.txt (una por linea: ID#ubicacion),
# asi solo hace falta escribir el ID.
#
# Uso:
#   ./estacion.sh WS-05            -> una estacion, con el menu del engine en esta terminal
#   ./estacion.sh todas            -> todas las del fichero, en segundo plano
#   ./estacion.sh todas 10         -> solo las 10 primeras
#   ./estacion.sh menu WS-05       -> abrir el menu de una que esta en segundo plano
#                                     (salir del menu sin pararla: Ctrl+P y luego Ctrl+Q)
#   ./estacion.sh parar            -> apagar todas
#
# Por defecto usa la Central y el Kafka del docker compose local. Para usar otros (ej. Railway):
#   CENTRAL=host:puerto KAFKA=host:puerto ./estacion.sh todas

CENTRAL="${CENTRAL:-central:9000}"
KAFKA="${KAFKA:-kafka:19092}"
cd "$(dirname "$0")" || exit 1          # docker compose se ejecuta en la raiz del repo
FICHERO="WM_WS/estaciones.txt"

# nombre de los contenedores: WS-05 -> mon-ws-05 y eng-ws-05 (igual para todas)
nombre() { echo "$1" | tr 'A-Z' 'a-z'; }

# ubicacion de una estacion segun el fichero
ubicacion() { grep -m1 "^$1#" "$FICHERO" | cut -d'#' -f2; }

# arranca el monitor de una estacion en segundo plano
lanzar_monitor() {
    docker compose run -d --rm --no-deps --name "mon-$(nombre "$1")" -e JAVA_TOOL_OPTIONS=-Xmx32m \
        monitor 8001 "$CENTRAL" "$1" "$2" < /dev/null > /dev/null
}

case "$1" in

    # ---------- todas las del fichero en segundo plano ----------
    todas)
        head -n "${2:-1000}" "$FICHERO" | while IFS='#' read -r ID UBI; do
            [ -z "$ID" ] && continue
            lanzar_monitor "$ID" "$UBI"
            docker compose run -d --rm --no-deps --name "eng-$(nombre "$ID")" -e JAVA_TOOL_OPTIONS=-Xmx64m \
                engine "$KAFKA" "mon-$(nombre "$ID"):8001" < /dev/null > /dev/null
            echo "$ID ($UBI) en marcha"
        done
        echo "Listo. Menu de una: ./estacion.sh menu WS-05   Apagarlas: ./estacion.sh parar"
        ;;

    # ---------- engancharse al menu de una que ya esta en marcha ----------
    menu)
        [ -z "$2" ] && { echo "Uso: ./estacion.sh menu WS-05"; exit 1; }
        echo "Menu de $2 (pulsa Enter para verlo; salir sin pararla: Ctrl+P y luego Ctrl+Q)"
        docker attach "eng-$(nombre "$2")"
        ;;

    # ---------- apagar todas ----------
    parar)
        docker rm -f $(docker ps -q --filter name=mon-ws --filter name=eng-ws) > /dev/null 2>&1
        echo "Estaciones apagadas"
        ;;

    # ---------- una estacion con su menu en esta terminal ----------
    "")
        echo "Uso: ./estacion.sh WS-05 | todas [n] | menu WS-05 | parar"
        exit 1
        ;;
    *)
        ID="$1"
        UBI="${2:-$(ubicacion "$ID")}"   # la ubicacion del fichero (o la que se escriba despues del id)
        [ -z "$UBI" ] && { echo "$ID no esta en $FICHERO (escribe tambien su ubicacion: ./estacion.sh $ID \"Parque...\")"; exit 1; }

        lanzar_monitor "$ID" "$UBI"
        echo "$ID ($UBI) en marcha"
        # al salir (Ctrl+C o fin del engine) apagar tambien su monitor
        trap 'docker rm -f "mon-$(nombre "$ID")" > /dev/null 2>&1' EXIT
        # engine en primer plano, con su menu
        docker compose run --rm --no-deps --name "eng-$(nombre "$ID")" engine "$KAFKA" "mon-$(nombre "$ID"):8001"
        ;;
esac
