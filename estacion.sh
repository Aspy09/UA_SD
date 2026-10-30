#!/usr/bin/env bash
# Lanza estaciones de riego (Monitor + Engine de cada una) con docker.
# La ubicacion y la region de cada estacion se cogen de WM_WS/estaciones.txt
# (una por linea: ID#ubicacion#region), asi solo hace falta escribir el ID.
#
# Uso:
#   ./estacion.sh WS-05            -> una estacion, y se abre su menu en esta terminal
#   ./estacion.sh todas            -> todas las del fichero, en segundo plano
#   ./estacion.sh todas 10         -> solo las 10 primeras
#   ./estacion.sh menu WS-05       -> abrir el menu de una que esta en segundo plano
#                                     (opcion 5 del menu para salir, la estacion sigue funcionando)
#   ./estacion.sh parar            -> apagar todas
#
# Por defecto usa la Central y el Kafka del docker compose local. Para usar otros (ej. Railway):
#   CENTRAL=host:puerto KAFKA=host:puerto ./estacion.sh todas

# si solo se ha puesto una de las dos direcciones, seguro que falta la otra: no lanzo nada
# (pasaba que los monitores iban a railway y los engines al kafka local y los riegos no llegaban)
if [ -n "$CENTRAL" ] && [ -z "$KAFKA" ]; then echo "Falta export KAFKA=... (has puesto CENTRAL pero no KAFKA)"; exit 1; fi
if [ -z "$CENTRAL" ] && [ -n "$KAFKA" ]; then echo "Falta export CENTRAL=... (has puesto KAFKA pero no CENTRAL)"; exit 1; fi
CENTRAL="${CENTRAL:-central:9000}"
KAFKA="${KAFKA:-kafka:19092}"
[ "$1" != "parar" ] && echo "Usando CENTRAL=$CENTRAL  KAFKA=$KAFKA"
cd "$(dirname "$0")" || exit 1          # docker compose se ejecuta en la raiz del repo
FICHERO="WM_WS/estaciones.txt"

# nombre de los contenedores: WS-05 -> mon-ws-05 y eng-ws-05 (igual para todas)
nombre() { echo "$1" | tr 'A-Z' 'a-z'; }

# ubicacion y region de una estacion segun el fichero
ubicacion() { grep -m1 "^$1#" "$FICHERO" | cut -d'#' -f2; }
region() { grep -m1 "^$1#" "$FICHERO" | cut -d'#' -f3; }

# si la estacion ya esta en marcha (su monitor existe) y si su engine esta en marcha
en_marcha() { [ -n "$(docker ps -q --filter "name=^mon-$(nombre "$1")$")" ]; }
engine_en_marcha() { [ -n "$(docker ps -q --filter "name=^eng-$(nombre "$1")$")" ]; }

# --progress quiet: que no salgan los mensajes de "Container ... Creating/Created"
dc() { docker compose --progress quiet "$@"; }

# arranca el monitor de una estacion en segundo plano (id, ubicacion, region)
lanzar_monitor() {
    dc run -d --rm --no-deps --name "mon-$(nombre "$1")" -e JAVA_TOOL_OPTIONS=-Xmx32m \
        monitor 8001 "$CENTRAL" "$1" "$2" "$3" < /dev/null > /dev/null
}

# arranca el engine de una estacion en segundo plano, conectado a su monitor
lanzar_engine() {
    # volumen wm-riegos: el engine apunta ahi el riego en marcha, si se cae lo recupera al volver
    dc run -d --rm --no-deps --name "eng-$(nombre "$1")" -e JAVA_TOOL_OPTIONS=-Xmx64m -v wm-riegos:/app/datos \
        engine "$KAFKA" "mon-$(nombre "$1"):8001" < /dev/null > /dev/null
}

# escribir una linea en el teclado del engine: se mete directamente en la entrada de java (proceso 1)
escribir() { printf '%s\n' "$2" | docker exec -i "eng-$(nombre "$1")" sh -c 'cat > /proc/1/fd/0'; }

# usar el menu del engine de una estacion que ya esta en marcha
# (sin docker attach: al desconectarse docker le cierra la entrada al engine y la siguiente vez el menu ya no va)
abrir_menu() {
    echo "Menu de $1 (opcion 5 para salir, la estacion sigue funcionando)"
    FIN=""
    trap 'FIN=1' USR1

    # lo que escribe el engine sale por pantalla; cuando dice que se sale del menu, se avisa al script
    docker logs -f --tail 0 "eng-$(nombre "$1")" 2>&1 | tee /dev/tty | { grep -q "Has salido del menu" && kill -USR1 $$; } &
    sleep 1
    escribir "$1" ""   # Enter para que muestre el menu

    # lo que escribes tu le llega al engine
    while [ -z "$FIN" ] && engine_en_marcha "$1"; do
        if IFS= read -r -t 1 LINEA; then escribir "$1" "$LINEA"; fi
    done
    # parar el docker logs de esta estacion
    pkill -f "docker logs -f --tail 0 eng-$(nombre "$1")" 2>/dev/null
    trap - USR1
    echo
}

case "$1" in

    # ---------- todas las del fichero en segundo plano ----------
    todas)
        head -n "${2:-1000}" "$FICHERO" | while IFS='#' read -r ID UBI REG; do
            [ -z "$ID" ] && continue
            if en_marcha "$ID"; then
                # el monitor sigue: si se ha caido su engine se vuelve a arrancar
                if engine_en_marcha "$ID"; then
                    echo "$ID ya estaba en marcha"
                else
                    lanzar_engine "$ID"
                    echo "$ID: engine arrancado otra vez"
                fi
                continue
            fi
            docker rm -f "eng-$(nombre "$ID")" > /dev/null 2>&1   # por si quedo un engine sin su monitor
            # si el monitor no arranca (por ejemplo docker aun esta borrando el anterior) lo intento otra vez
            if ! lanzar_monitor "$ID" "$UBI" "$REG"; then
                sleep 2
                lanzar_monitor "$ID" "$UBI" "$REG" || { echo "$ID: no se ha podido arrancar el monitor"; continue; }
            fi
            lanzar_engine "$ID"
            echo "$ID ($UBI, region $REG) en marcha"
        done
        echo "Listo. Menu de una: ./estacion.sh menu WS-05   Apagarlas: ./estacion.sh parar"
        ;;

    # ---------- engancharse al menu de una que ya esta en marcha ----------
    menu)
        [ -z "$2" ] && { echo "Uso: ./estacion.sh menu WS-05"; exit 1; }
        en_marcha "$2" || { echo "$2 no esta en marcha. Lanzala con: ./estacion.sh todas  (o ./estacion.sh $2)"; exit 1; }
        # si su engine se ha caido, volver a arrancarlo antes de abrir el menu
        if ! engine_en_marcha "$2"; then
            lanzar_engine "$2"
            echo "El engine de $2 no estaba, lo arranco otra vez"
            sleep 3
        fi
        abrir_menu "$2"
        ;;

    # ---------- apagar todas ----------
    parar)
        docker rm -f $(docker ps -aq --filter name=mon-ws --filter name=eng-ws) > /dev/null 2>&1
        # esperar a que docker termine de borrarlos, si no al volver a lanzarlas chocan los nombres
        while [ -n "$(docker ps -aq --filter name=mon-ws --filter name=eng-ws)" ]; do sleep 1; done
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
        REG="${3:-$(region "$ID")}"      # la region del fichero (o la que se escriba despues)
        [ -z "$UBI" ] && { echo "$ID no esta en $FICHERO (escribe tambien su ubicacion: ./estacion.sh $ID \"Parque...\")"; exit 1; }

        # si ya esta en marcha (por ejemplo con "todas") no se lanza otra vez, se abre su menu
        if en_marcha "$ID"; then
            echo "$ID ya esta en marcha. Para usar su menu: ./estacion.sh menu $ID"
            exit 1
        fi
        docker rm -f "eng-$(nombre "$ID")" > /dev/null 2>&1   # por si quedo un engine sin su monitor
        lanzar_monitor "$ID" "$UBI" "$REG" || { echo "No se ha podido arrancar el monitor de $ID"; exit 1; }
        lanzar_engine "$ID"
        echo "$ID ($UBI, region $REG) en marcha (para apagarla: ./estacion.sh parar)"
        sleep 3   # que el engine arranque antes de abrir su menu
        abrir_menu "$ID"
        ;;
esac
