#!/usr/bin/env bash
# Menu de la central en la terminal. Manda las ordenes a la central (la de railway por defecto)
# por la misma direccion que usan los botones del panel (/api/orden), asi se ve todo en el panel.
#
# Uso:
#   ./central.sh                              -> central de railway
#   PANEL=http://localhost:8080 ./central.sh  -> central local de docker compose

PANEL="${PANEL:-https://wmcentral-production-1fde.up.railway.app}"
echo "Usando la central de $PANEL"

# mandar una orden: accion, estacion (o TODAS), provincia, segundos
orden() {
    RESPUESTA=$(curl -s -m 10 -X POST "$PANEL/api/orden?accion=$1&ws=$2&region=$3&segundos=$4")
    if [ "$RESPUESTA" = "OK" ]; then
        echo "Orden enviada"
    else
        echo "No se ha podido: ${RESPUESTA:-la central no contesta}"
    fi
}

# lista de estaciones: del json de /api/estado saco id, ubicacion, provincia y estado de cada una
ver_estaciones() {
    curl -s -m 10 "$PANEL/api/estado" \
        | grep -o '"id":"[^"]*","ubicacion":"[^"]*","region":"[^"]*","estado":"[^"]*"' \
        | sed 's/"id":"\([^"]*\)","ubicacion":"\([^"]*\)","region":"\([^"]*\)","estado":"\([^"]*\)"/\1 - \2 (\3): \4/'
}

# preguntar a que estacion va la orden (con TODAS tambien la provincia)
pedir_estacion() {
    read -r -p "Estacion (WS-05 o TODAS): " WS
    WS=$(echo "$WS" | tr 'a-z' 'A-Z')
    REGION=""
    if [ "$WS" = "TODAS" ]; then
        read -r -p "Provincia (Enter = todas las provincias): " REGION
        REGION=$(echo "$REGION" | tr 'a-z' 'A-Z')
    fi
}

while true; do
    echo
    echo "=== CENTRAL ==="
    echo "1) Ver estaciones  2) Regar  3) Bloquear (fuera de servicio)  4) Activar  5) Dar de baja  6) Salir"
    read -r -p "Opcion: " OPCION || exit 0
    case "$OPCION" in
        1) ver_estaciones ;;
        2) pedir_estacion
           read -r -p "Segundos de riego: " SEG
           orden riego "$WS" "$REGION" "$SEG" ;;
        3) pedir_estacion; orden bloquear "$WS" "$REGION" ;;
        4) pedir_estacion; orden activar "$WS" "$REGION" ;;
        5) pedir_estacion; orden baja "$WS" "$REGION" ;;
        6) exit 0 ;;
        "") ;;
        *) echo "Opcion no valida" ;;
    esac
done
