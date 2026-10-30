# Protocolo de comunicación — WaterManagement

Este documento define cómo se hablan los módulos. **Cualquier cambio aquí se avisa a la otra persona antes de programarlo.**

---

## 1. Reglas generales

- Los campos de cada mensaje se separan con `#`. El primer campo es siempre el **código de operación**.
- Texto en ASCII, sin tildes ni `#` dentro de los campos (ej. ubicación `Parque Norte`, no `Parque Nórte`).
- IDs: estaciones `WS-01`, `WS-02`…; operarios `FO-01`, `FO-02`…; la propia central se identifica como `CENTRAL`.

### Estados de una WS

| Estado | Color en el panel | Significado |
|---|---|---|
| `DISPONIBLE` | Verde | Conectada, sin averías, esperando riego |
| `REGANDO` | Verde parpadeando | Regando ahora mismo |
| `FUGA` | Rojo | Avería detectada; no puede regar |
| `FUERA_DE_SERVICIO` | Naranja | Bloqueada por CENTRAL |
| `DESCONECTADA` | Gris | Su Monitor no está conectado a CENTRAL |

---

## 2. Sockets: formato de trama

Se usa en **Monitor ↔ CENTRAL** y en **Engine ↔ Monitor**.

```
<STX><DATA><ETX><LRC>
```

| Símbolo | Byte | Para qué sirve |
|---|---|---|
| `STX` | `0x02` | Inicio de mensaje |
| `ETX` | `0x03` | Fin de mensaje |
| `LRC` | 1 byte | XOR de todos los bytes de `DATA` |
| `ENQ` | `0x05` | "¿Estás ahí?" al abrir la conexión |
| `ACK` | `0x06` | Trama recibida correctamente |
| `NACK` | `0x15` | Trama corrupta (el LRC no coincide) → el emisor la reenvía |
| `EOT` | `0x04` | Fin de la comunicación, se va a cerrar |

**Ejemplo:** `DATA = "OK#WS-01"` → LRC = `0x0F` → se envía `02 'OK#WS-01' 03 0F`.

### Flujo

La conexión es **persistente**: se abre una vez y se mantiene. Si se cierra sin `EOT` (el `readLine`/`read` devuelve fin de flujo), el otro extremo lo trata como **desconexión**.

```
Cliente                         Servidor
  |--- ENQ ------------------------>|
  |<-------------------------- ACK -|
  |                                 |
  |--- STX PETICION ETX LRC ------->|   ┐
  |<-------------------------- ACK -|   │ se repite
  |<------ STX RESPUESTA ETX LRC ---|   │ tantas veces
  |--- ACK ------------------------>|   ┘ como haga falta
  |                                 |
  |--- EOT ------------------------>|   (solo al cerrar de forma ordenada)
```

Cualquiera de los dos extremos puede iniciar una petición (ej. el Monitor manda `AVERIA` cuando quiera; el Monitor pregunta `SALUD` al Engine cada segundo).

---

## 3. Monitor (WM_WS_M) ↔ CENTRAL (WM_Central)

El Monitor es el **cliente**; CENTRAL es el **servidor** (puerto pasado por argumento).

Argumentos del Monitor: `<puerto_para_engine> <ip_central:puerto> <id_ws> <ubicacion> [provincia]`
(la provincia es una de `ALICANTE`, `VALENCIA`, `MADRID`, `BARCELONA`, `SEVILLA`; si no se pone, `ALICANTE`).
(la ubicación se añade como 4º argumento porque el enunciado pide enviarla al registrarse).

| Petición (Monitor → CENTRAL) | Respuesta (CENTRAL → Monitor) | Qué hace CENTRAL |
|---|---|---|
| `REGISTRO#<id_ws>#<ubicacion>#<provincia>` | `OK#<id_ws>` | Si el ID no existe en BD, lo da de alta (con su provincia). Lo marca `DISPONIBLE` (o `FUERA_DE_SERVICIO` si estaba bloqueada). |
| | `KO#<id_ws>#REGION_DESCONOCIDA` | Si la provincia no es una de las 5. |
| | `KO#<id_ws>#<motivo>` | Ej. `KO#WS-01#YA_CONECTADA` si ya hay otro Monitor con ese ID. El Monitor termina. |
| `AVERIA#<id_ws>` | `OK#<id_ws>` | Marca `FUGA`. Si estaba regando, ordena `PARAR`. |
| `RESUELTO#<id_ws>` | `OK#<id_ws>` | Marca `DISPONIBLE`. |

Desconexión (socket cerrado) → CENTRAL marca la WS como `DESCONECTADA`.

---

## 4. Engine (WM_WS_E) ↔ Monitor (WM_WS_M)

El Engine es el **cliente**; el Monitor es el **servidor**.

| Petición | Respuesta | Notas |
|---|---|---|
| Engine → Monitor: `CONECTAR` | `ID#<id_ws>#<ubicacion>#<provincia>` | Así el Engine sabe qué WS es (el ID lo valida CENTRAL al registrar el Monitor) y de qué provincia, para leer solo sus órdenes. |
| | `KO#NO_REGISTRADA` | Si CENTRAL aún no ha aceptado el registro. El Engine reintenta. |
| Monitor → Engine (cada 1 s): `SALUD#<id_ws>` | `OK#<id_ws>` o `KO#<id_ws>` | El Engine responde `KO` mientras el usuario tenga simulada la avería (tecla). |

Si el Engine responde `KO` o no responde en ~3 s → el Monitor envía `AVERIA` a CENTRAL.
Cuando vuelve a responder `OK` → el Monitor envía `RESUELTO`.

---

## 5. Kafka

En Kafka **no** se usa STX/ETX/LRC: Kafka ya separa los mensajes y garantiza su integridad. Se envía solo el texto `CODIGO#campo#...`.

Como **key** de cada mensaje se pone el ID de la WS (o del FO en `wm.notificaciones`), para que los mensajes de una misma WS lleguen en orden.

| Topic | Quién escribe → quién lee |
|---|---|
| `wm.solicitudes` | FO y Engine → CENTRAL |
| `wm.ordenes.<PROVINCIA>` | CENTRAL → Engines de esa provincia (uno por provincia: `wm.ordenes.ALICANTE`, `wm.ordenes.MADRID`...) |
| `wm.telemetria` | Engines → CENTRAL |
| `wm.notificaciones` | CENTRAL → FO |
| `wm.estaciones` | CENTRAL → FO (estado de todas las WS) |
| `wm.errores` | Engines y FO → CENTRAL (sus errores, salen en las alertas del panel) |

**group.id:** CENTRAL usa `central`. Cada Engine usa su ID (`WS-01`) y cada FO el suyo (`FO-01`), para que **todos** reciban todos los mensajes del topic y cada uno filtre los que le tocan.

**Escalabilidad (un topic de órdenes por provincia):** si todos los Engines leyeran un único `wm.ordenes`, con millones de estaciones cada Engine recibiría las órdenes de todas. Por eso hay un topic por provincia y cada Engine solo se suscribe al de la suya: recibe únicamente las órdenes de las estaciones de su provincia.

### `wm.solicitudes` (FO / Engine → CENTRAL)
| Mensaje | Significado |
|---|---|
| `RIEGO#<origen>#<id_ws>#<segundos>` | Pide regar `<id_ws>` durante `<segundos>`. `<origen>` = ID del FO, o el ID de la WS si se pide desde el menú de la estación. |

### `wm.ordenes.<PROVINCIA>` (CENTRAL → Engine)
| Mensaje | Significado |
|---|---|
| `INICIAR#<id_ws>#<origen>#<segundos>` | Abrir electroválvula. `<origen>` = FO, WS o `CENTRAL`. |
| `PARAR#<id_ws>` | Cerrar electroválvula (por avería). |
| `BLOQUEAR#<id_ws>` | Parar si riega y pasar a `FUERA_DE_SERVICIO`. |
| `ACTIVAR#<id_ws>` | Volver a `DISPONIBLE`. |

`<id_ws>` = `TODAS` para enviar la orden a todas las estaciones.

### `wm.telemetria` (Engine → CENTRAL)
| Mensaje | Cuándo |
|---|---|
| `INICIADO#<id_ws>` | Ha abierto la electroválvula tras `INICIAR`. |
| `RECHAZADO#<id_ws>#<motivo>` | No puede regar (ej. `FUGA`). |
| `RIEGO#<id_ws>#<caudal_l_min>#<volumen_l>` | Cada segundo mientras riega. Decimales con punto: `11.4`. |
| `FIN#<id_ws>#<volumen_total_l>#<duracion_s>#<motivo>` | Fin del riego. Motivo: `TIEMPO`, `MANUAL`, `AVERIA` o `BLOQUEO`. |

### `wm.notificaciones` (CENTRAL → FO)
| Mensaje | Cuándo |
|---|---|
| `INFO#<id_fo>#<id_ws>#<texto>` | Pasos intermedios ("Validando estacion", "Esperando confirmacion de la WS"…). |
| `AUTORIZADO#<id_fo>#<id_ws>` | La WS ha confirmado `INICIADO`. |
| `DENEGADO#<id_fo>#<id_ws>#<motivo>` | Motivo: `NO_EXISTE`, `DESCONECTADA`, `FUGA`, `FUERA_DE_SERVICIO`, `OCUPADA`, `RECHAZADO`, `OPERARIO_NO_EXISTE`, `ERROR_BD`, `OTRA_PROVINCIA` (el operario pide una estación que no es de su provincia), `SIN_RESPUESTA` (la estación no confirmó el riego en 15 s: se cancela la reserva). |
| `RIEGO#<id_fo>#<id_ws>#<caudal>#<volumen>` | Reenvío de la telemetría solo al FO que pidió el riego. |
| `AVERIA#<id_fo>#<id_ws>` | La WS se ha averiado durante el riego. El RESUMEN llega cuando la WS lo cierre (si el Engine se ha caído, al volver). |
| `RESUMEN#<id_fo>#<id_ws>#<volumen_total>#<duracion_s>#<motivo>` | Al terminar. Tras esto el FO espera 4 s y pide el siguiente servicio del fichero. |

### `wm.estaciones` (CENTRAL → todos los FO)
| Mensaje | Cuándo |
|---|---|
| `ESTADO#<id_ws>#<ubicacion>#<estado>#<provincia>` | Cada vez que cambia el estado de una WS (y una vez por WS al arrancar CENTRAL). Cada FO solo guarda las de su provincia. |
| `BAJA#<id_ws>` | Cuando se da de baja una WS desde el panel. Los FO la quitan de su lista. |

---

### `wm.errores` (Engines y FO → CENTRAL)
| Mensaje | Cuándo |
|---|---|
| `ERROR#<quien>#<texto>` | Un módulo tiene un problema: el Engine pierde su Monitor, el FO no puede leer su fichero o tiene una línea mal escrita, piden una estación o unos segundos no válidos... La CENTRAL lo muestra en **Alertas**. |

### Trazabilidad
Cada riego que llega a la CENTRAL recibe un número de operación (`OP-0001`, `OP-0002`...). La CENTRAL apunta con la hora cada paso de esa operación (solicitud, validación, `INICIAR`, `INICIADO`, telemetría, `FIN`, `RESUMEN` o `DENEGADO`) y el panel muestra la historia completa de cada riego.

### Órdenes de la CENTRAL
Las órdenes a una o todas las WS (regar, bloquear, activar, dar de baja) se pueden dar desde el panel web o desde el menú de la CENTRAL en su terminal. Con `TODAS` se puede elegir también una provincia. Si la CENTRAL corre en docker, al menú se entra con `docker attach` a su contenedor. Con la CENTRAL en Railway (sin terminal) se usa `./central.sh`: el mismo menú en tu terminal, que manda las órdenes a `/api/orden` igual que los botones del panel (`PANEL=http://localhost:8080 ./central.sh` para la local).

### Operarios
Los operarios que existen están en `WM_Central/src/main/resources/operarios.txt` (`FO-01#nombre#provincia`, 3 por provincia). La CENTRAL los da de alta en la BD al arrancar. Un operario solo ve y solo puede regar las estaciones de su provincia (`operario.sh` le pasa la provincia en la variable `REGION`; aunque no la tenga, la CENTRAL lo comprueba y deniega con `OTRA_PROVINCIA`). Para salir: opción 3 del menú del operario y opción 5 del menú de la estación (la estación sigue funcionando).

### Resiliencia
- Si cae el **Monitor** durante un riego, el Engine corta el riego (`FIN#...#MONITOR`, la CENTRAL lo marca `CORTADA (MONITOR)` y el FO recibe su RESUMEN) y se queda averiado sin dejar regar hasta que el Monitor vuelva.
- Si cae el **Engine** durante un riego, el Monitor avisa de la avería a la CENTRAL y esta manda `AVERIA` al FO. El Engine apunta cada segundo el riego en marcha en `datos/riego_<id>.txt` (en docker es el volumen `wm-riegos`); al volver encuentra el fichero y manda `FIN#...#CAIDA_ENGINE` con lo que se había regado, así el FO recibe su RESUMEN.
- Si un **FO** se cierra durante un riego, el riego sigue. Al volver, Kafka le da los avisos desde donde lo dejó (su grupo guarda el offset), así que ve el resultado.
- Si la **CENTRAL** no contesta a una petición en 25 s, el FO muestra `Imposible conectar con WM_Central`.

## 6. Ejemplo completo: un riego pedido por un operario

```
FO-01   → wm.solicitudes     RIEGO#FO-01#WS-04#60
CENTRAL → wm.notificaciones  INFO#FO-01#WS-04#Validando estacion
          (CENTRAL comprueba en su estado que WS-04 está DISPONIBLE)
CENTRAL → wm.ordenes.ALICANTE INICIAR#WS-04#FO-01#60
WS-04   → wm.telemetria      INICIADO#WS-04
CENTRAL → wm.notificaciones  AUTORIZADO#FO-01#WS-04
WS-04   → wm.telemetria      RIEGO#WS-04#11.4#0.2      (cada segundo)
CENTRAL → wm.notificaciones  RIEGO#FO-01#WS-04#11.4#0.2
   ... 60 segundos ...
WS-04   → wm.telemetria      FIN#WS-04#684.0#60#TIEMPO
CENTRAL → wm.notificaciones  RESUMEN#FO-01#WS-04#684.0#60#TIEMPO
```
