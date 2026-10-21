#!/usr/bin/env bash
# =====================================================================
# Pruebas automaticas de WaterManagement (en local, con docker compose)
#
# Uso (desde la raiz del repo, con el docker compose normal PARADO):
#     ./pruebas/pruebas.sh
#
# Levanta Kafka + Central en un proyecto aparte ("wmtest", con su propia
# base de datos vacia), lanza estaciones y operarios, provoca situaciones
# y comprueba en los logs que el sistema reacciona como debe.
# Los logs quedan en pruebas/logs/ para revisarlos.
# =====================================================================

cd "$(dirname "$0")/.." || exit 1
DC="docker compose -p wmtest"
LOGS="pruebas/logs"
OK=0
FALLOS=0

# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------

titulo() { echo; echo "=== $1 ==="; }

bien()  { echo "  ✅ $1"; OK=$((OK + 1)); }
mal()   { echo "  ❌ $1"; FALLOS=$((FALLOS + 1)); }

# Numero de lineas que tiene ya un log (para buscar solo lo que venga despues)
marca() { wc -l < "$1" 2>/dev/null || echo 0; }

# esperar <log> <desde_linea> <texto> <segundos>: espera a que aparezca el texto
esperar() {
    local log="$1" desde="$2" texto="$3" max="$4" t=0
    while [ "$t" -lt "$max" ]; do
        if tail -n +"$((desde + 1))" "$log" 2>/dev/null | grep -qF -- "$texto"; then
            return 0
        fi
        sleep 1
        t=$((t + 1))
    done
    return 1
}

# comprobar <descripcion> <log> <desde_linea> <texto> <segundos>
comprobar() {
    if esperar "$2" "$3" "$4" "$5"; then bien "$1"; else mal "$1  (no aparece: '$4' en $2)"; fi
}

# no_aparece <descripcion> <log> <texto>
no_aparece() {
    if grep -qF -- "$3" "$2" 2>/dev/null; then mal "$1  (aparece '$3' en $2)"; else bien "$1"; fi
}

# Lanza un monitor en segundo plano con su log
lanzar_monitor() {   # id ubicacion
    local nombre="t-monitor-$(echo "$1" | tr 'A-Z' 'a-z')"
    docker rm -f "$nombre" > /dev/null 2>&1
    $DC run -d --rm --no-deps --name "$nombre" monitor 8001 central:9000 "$1" "$2" > /dev/null
    docker logs -f "$nombre" >> "$LOGS/monitor-$1.log" 2>&1 &
}

# Lanza un engine leyendo su teclado de una tuberia (para poder "pulsar" opciones)
lanzar_engine() {    # id
    local id="$1" min
    min="$(echo "$id" | tr 'A-Z' 'a-z')"
    rm -f "$LOGS/teclado-$id"
    mkfifo "$LOGS/teclado-$id"
    $DC run -T --rm --no-deps --name "t-engine-$min" engine kafka:19092 "t-monitor-$min:8001" \
        < "$LOGS/teclado-$id" > "$LOGS/engine-$id.log" 2>&1 &
}

# Lanza un operario en modo fichero; termina solo al acabar el fichero
lanzar_fo() {        # id_fo nombre_log lineas...
    local id="$1" nombre="$2"
    shift 2
    printf '%s\n' "$@" > "$LOGS/$nombre.txt"
    timeout 300 $DC run -T --rm --no-deps -v "$(realpath "$LOGS/$nombre.txt"):/app/peticiones.txt:ro" \
        fo kafka:19092 "$id" peticiones.txt < /dev/null > "$LOGS/$nombre.log" 2>&1 &
}

limpiar() {
    echo
    echo "Limpiando contenedores de prueba..."
    exec 3>&- 4>&- 5>&- 2>/dev/null
    docker ps -aq --filter "name=t-monitor-" --filter "name=t-engine-" | xargs -r docker rm -f > /dev/null 2>&1
    $DC --profile interactivo down -v --remove-orphans > /dev/null 2>&1
    jobs -p | xargs -r kill > /dev/null 2>&1
}
trap limpiar EXIT

# ---------------------------------------------------------------------
# Preparacion
# ---------------------------------------------------------------------

if ss -ltn 2>/dev/null | grep -qE ':(9000|9092)\s'; then
    echo "Los puertos 9000 o 9092 estan ocupados. Para antes el docker compose normal"
    echo "(docker compose down) o cualquier Kafka/Central que tengas abierta."
    exit 1
fi

rm -rf "$LOGS"
mkdir -p "$LOGS"

titulo "Construyendo imagenes (la primera vez tarda)"
$DC --profile interactivo build -q || { echo "Fallo al construir"; exit 1; }

titulo "1. Arranque de Kafka y Central"
$DC down -v > /dev/null 2>&1
$DC up -d kafka central > /dev/null 2>&1
$DC logs -f --no-color central > "$LOGS/central.log" 2>&1 &
comprobar "Central arranca y conecta con Kafka" "$LOGS/central.log" 0 "Central escuchando en el puerto 9000" 90

# ---------------------------------------------------------------------
titulo "2. Registro de una estacion (WS-01)"
c=$(marca "$LOGS/central.log")
lanzar_monitor WS-01 "Parque Norte"
lanzar_engine WS-01
exec 3> "$LOGS/teclado-WS-01"          # descriptor 3 = teclado del engine WS-01
comprobar "Central registra WS-01 como DISPONIBLE" "$LOGS/central.log" "$c" "WS-01 (Parque Norte) -> DISPONIBLE" 30
comprobar "El engine se conecta al monitor" "$LOGS/engine-WS-01.log" 0 "Conectado al Monitor" 30
comprobar "El engine esta listo para recibir ordenes" "$LOGS/engine-WS-01.log" 0 "Listo para recibir ordenes" 30

# ---------------------------------------------------------------------
titulo "3. Riego completo pedido por un operario (fichero)"
lanzar_fo FO-01 fo-riego "WS-01#5"
comprobar "El operario recibe AUTORIZADO" "$LOGS/fo-riego.log" 0 "Riego AUTORIZADO" 40
comprobar "El operario recibe caudal y volumen" "$LOGS/fo-riego.log" 0 "L/min - Volumen" 20
comprobar "El riego termina por TIEMPO" "$LOGS/fo-riego.log" 0 "(TIEMPO)" 30
comprobar "El engine envia FIN" "$LOGS/engine-WS-01.log" 0 "FIN#WS-01#" 5

# ---------------------------------------------------------------------
titulo "4. Peticiones incorrectas (no debe explotar nada)"
lanzar_fo FO-01 fo-errores "Estacion 1" "WS-99#5" "WS-01#abc" ""
comprobar "Linea sin formato ignorada" "$LOGS/fo-errores.log" 0 "ignorada" 40
comprobar "Estacion inexistente -> DENEGADO NO_EXISTE" "$LOGS/fo-errores.log" 0 "DENEGADO: NO_EXISTE" 40
comprobar "Segundos no validos detectados" "$LOGS/fo-errores.log" 0 "Segundos no validos" 30
comprobar "El fichero termina" "$LOGS/fo-errores.log" 0 "Fichero terminado" 30
lanzar_fo FO-09 fo-inexistente "WS-01#5"
comprobar "Operario inexistente -> DENEGADO OPERARIO_NO_EXISTE" "$LOGS/fo-inexistente.log" 0 "OPERARIO_NO_EXISTE" 40
sleep 5

# ---------------------------------------------------------------------
titulo "5. Averia durante un riego"
c=$(marca "$LOGS/central.log")
lanzar_fo FO-01 fo-averia "WS-01#60"
comprobar "Empieza un riego largo" "$LOGS/fo-averia.log" 0 "Riego AUTORIZADO" 40
sleep 3
echo "3" >&3                             # menu del engine: simular averia
comprobar "El riego se corta por AVERIA" "$LOGS/fo-averia.log" 0 "(AVERIA)" 15
comprobar "Central marca WS-01 en FUGA" "$LOGS/central.log" "$c" "WS-01 (Parque Norte) -> FUGA" 15
sleep 5
lanzar_fo FO-01 fo-con-fuga "WS-01#5"
comprobar "Con averia, Central deniega (FUGA)" "$LOGS/fo-con-fuga.log" 0 "DENEGADO: FUGA" 40
c=$(marca "$LOGS/central.log")
echo "3" >&3                             # resolver averia
comprobar "Al resolverla vuelve a DISPONIBLE" "$LOGS/central.log" "$c" "WS-01 (Parque Norte) -> DISPONIBLE" 15
sleep 5

# ---------------------------------------------------------------------
titulo "6. Riego pedido desde la estacion y parada manual"
c=$(marca "$LOGS/central.log")
e=$(marca "$LOGS/engine-WS-01.log")
printf '1\n60\n' >&3                     # menu: pedir riego de 60 s
comprobar "Central recibe la peticion de la propia estacion" "$LOGS/central.log" "$c" "WS-01 pide regar WS-01" 15
comprobar "El engine empieza a regar" "$LOGS/engine-WS-01.log" "$e" "Riego iniciado" 15
sleep 3
echo "2" >&3                             # menu: parar riego
comprobar "El riego termina como MANUAL" "$LOGS/engine-WS-01.log" "$e" "#MANUAL" 10
sleep 5

# ---------------------------------------------------------------------
titulo "7. Caida y vuelta de CENTRAL (resiliencia)"
m=$(marca "$LOGS/monitor-WS-01.log")
$DC stop central > /dev/null 2>&1
comprobar "El monitor detecta la caida y sigue vivo" "$LOGS/monitor-WS-01.log" "$m" "Conexion con CENTRAL perdida" 20
$DC start central > /dev/null 2>&1
$DC logs -f --no-color --since 1s central >> "$LOGS/central.log" 2>&1 &
comprobar "Al volver Central, el monitor se registra solo" "$LOGS/monitor-WS-01.log" "$m" "registrada en CENTRAL" 60
lanzar_fo FO-01 fo-tras-caida "WS-01#3"
comprobar "Tras la caida se puede volver a regar" "$LOGS/fo-tras-caida.log" 0 "(TIEMPO)" 60
sleep 5

# ---------------------------------------------------------------------
titulo "8. Caida y vuelta del MONITOR (resiliencia)"
c=$(marca "$LOGS/central.log")
e=$(marca "$LOGS/engine-WS-01.log")
docker rm -f t-monitor-ws-01 > /dev/null 2>&1
comprobar "Central marca WS-01 DESCONECTADA" "$LOGS/central.log" "$c" "WS-01 (Parque Norte) -> DESCONECTADA" 15
comprobar "El engine sigue vivo y reintenta" "$LOGS/engine-WS-01.log" "$e" "Reintentando" 15
lanzar_monitor WS-01 "Parque Norte"
comprobar "El engine se reconecta al nuevo monitor" "$LOGS/engine-WS-01.log" "$e" "Conectado al Monitor" 30
comprobar "WS-01 vuelve a DISPONIBLE" "$LOGS/central.log" "$c" "WS-01 (Parque Norte) -> DISPONIBLE" 30

# ---------------------------------------------------------------------
titulo "9. Escalabilidad: 3 estaciones y 3 operarios a la vez"
for ws in WS-02 WS-03; do
    lanzar_monitor "$ws" "Parque $ws"
    lanzar_engine "$ws"
done
exec 4> "$LOGS/teclado-WS-02"          # mantener abiertos sus teclados
exec 5> "$LOGS/teclado-WS-03"
comprobar "WS-02 lista" "$LOGS/engine-WS-02.log" 0 "Listo para recibir ordenes" 40
comprobar "WS-03 lista" "$LOGS/engine-WS-03.log" 0 "Listo para recibir ordenes" 40
lanzar_fo FO-01 fo-a "WS-01#6"
lanzar_fo FO-02 fo-b "WS-02#6"
lanzar_fo FO-03 fo-c "WS-03#6"
comprobar "FO-01 riega WS-01" "$LOGS/fo-a.log" 0 "[WS-01] FIN DEL RIEGO" 60
comprobar "FO-02 riega WS-02" "$LOGS/fo-b.log" 0 "[WS-02] FIN DEL RIEGO" 60
comprobar "FO-03 riega WS-03" "$LOGS/fo-c.log" 0 "[WS-03] FIN DEL RIEGO" 60
no_aparece "FO-01 no ve los riegos de los demas" "$LOGS/fo-a.log" "[WS-02]"
no_aparece "FO-02 no ve los riegos de los demas" "$LOGS/fo-b.log" "[WS-03]"
sleep 5

# ---------------------------------------------------------------------
titulo "10. Dos operarios piden la misma estacion"
lanzar_fo FO-02 fo-d "WS-02#15"
sleep 4
lanzar_fo FO-03 fo-e "WS-02#5"
comprobar "El segundo es denegado (OCUPADA)" "$LOGS/fo-e.log" 0 "DENEGADO: OCUPADA" 40
comprobar "El primero termina su riego" "$LOGS/fo-d.log" 0 "[WS-02] FIN DEL RIEGO" 40

# ---------------------------------------------------------------------
echo
echo "====================================="
echo "  Correctas: $OK   Fallidas: $FALLOS"
echo "  Logs en: $LOGS/"
echo "====================================="
[ "$FALLOS" -eq 0 ]