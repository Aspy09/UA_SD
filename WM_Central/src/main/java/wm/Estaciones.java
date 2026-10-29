package wm;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

// estado de todas las estaciones, lo comparten todos los hilos de la central
public class Estaciones {
    private static final ConcurrentHashMap<String, Estacion> estaciones = new ConcurrentHashMap<>();

    // al arrancar meter en el mapa las estaciones de la bd (desconectadas)
    public static synchronized void cargarDesdeBD() throws SQLException {
        for (Estacion ws : BaseDatos.cargarEstaciones()) {
            estaciones.put(ws.getId(), ws);
            System.out.println("[BD] " + ws);
        }
        System.out.println("[BD] " + estaciones.size() + " estaciones cargadas");
    }

    // cambiar el estado y avisar a los fo por wm.estaciones
    private static void cambiarEstado(Estacion ws, EstadoWS nuevo) {
        ws.setEstado(nuevo);
        // fuga y desconexion van a alertas, lo demas a actividad
        if (nuevo == EstadoWS.FUGA || nuevo == EstadoWS.DESCONECTADA) Mensajes.alerta(ws.toString());
        else Mensajes.add(ws.toString());
        publicar(ws);
    }

    private static void publicar(Estacion ws) {
        KafkaCentral.enviar(KafkaCentral.ESTACIONES, ws.getId(),
                "ESTADO#" + ws.getId() + "#" + ws.getUbicacion() + "#" + ws.getEstado());
    }

    // al arrancar mandar el estado de todas para que los fo las vean
    public static synchronized void publicarTodas() {
        for (Estacion ws : estaciones.values()) {
            publicar(ws);
        }
    }

    // registro de una ws, devuelve la respuesta para el monitor
    // synchronized para que no se registren dos monitores con el mismo id a la vez
    public static synchronized String registrar(String id, String ubicacion, String region) {
        Estacion ws = estaciones.get(id);
        if (ws == null) {
            ws = new Estacion(id, ubicacion);
            estaciones.put(id, ws);
            Mensajes.add("Nueva estacion " + id + " en " + ubicacion);
        } else if (ws.getEstado() != EstadoWS.DESCONECTADA) {
            return "KO#" + id + "#YA_CONECTADA";
        }
        ws.setUbicacion(ubicacion);
        ws.setRegion(region);
        guardar(ws);
        cambiarEstado(ws, ws.isBloqueada() ? EstadoWS.FUERA_DE_SERVICIO : EstadoWS.DISPONIBLE);
        return "OK#" + id;
    }

    public static synchronized void averia(String id) {
        Estacion ws = estaciones.get(id);
        // si estaba regando hay que parar el riego
        if (ws.getEstado() == EstadoWS.REGANDO) {
            KafkaCentral.enviar(KafkaCentral.ordenes(ws.getRegion()), id, "PARAR#" + id);
            Operaciones.paso(ws.getOperacion(), "PARAR enviado: fuga en la estacion mientras regaba");
        }
        cambiarEstado(ws, EstadoWS.FUGA);
    }

    public static synchronized void resuelto(String id) {
        Estacion ws = estaciones.get(id);
        cambiarEstado(ws, ws.isBloqueada() ? EstadoWS.FUERA_DE_SERVICIO : EstadoWS.DISPONIBLE);
    }

    public static synchronized void desconectar(String id) {
        Estacion ws = estaciones.get(id);
        cambiarEstado(ws, EstadoWS.DESCONECTADA);
    }

    // guardar en la bd, si falla la estacion sigue funcionando igual
    private static void guardar(Estacion ws) {
        try {
            BaseDatos.guardarEstacion(ws);
        } catch (SQLException e) {
            Mensajes.alerta("Error guardando " + ws.getId() + " en la bd: " + e.getMessage());
        }
    }

    // ordenes del panel (punto 11)

    // bloquear: fuera de servicio aunque se desconecte y vuelva
    public static synchronized boolean bloquear(String id) {
        Estacion ws = estaciones.get(id);
        if (ws == null) return false;
        ws.setBloqueada(true);
        guardar(ws);
        // el engine para el riego si estaba regando
        KafkaCentral.enviar(KafkaCentral.ordenes(ws.getRegion()), id, "BLOQUEAR#" + id);
        if (ws.getEstado() == EstadoWS.REGANDO) Operaciones.paso(ws.getOperacion(), "BLOQUEAR enviado: la central bloquea la estacion");
        // si tiene fuga o esta desconectada se queda asi, al volver ya saldra fuera de servicio
        if (ws.getEstado() == EstadoWS.DISPONIBLE || ws.getEstado() == EstadoWS.REGANDO) {
            cambiarEstado(ws, EstadoWS.FUERA_DE_SERVICIO);
        }
        return true;
    }

    public static synchronized boolean activar(String id) {
        Estacion ws = estaciones.get(id);
        if (ws == null) return false;
        ws.setBloqueada(false);
        guardar(ws);
        KafkaCentral.enviar(KafkaCentral.ordenes(ws.getRegion()), id, "ACTIVAR#" + id);
        if (ws.getEstado() == EstadoWS.FUERA_DE_SERVICIO) {
            cambiarEstado(ws, EstadoWS.DISPONIBLE);
        }
        return true;
    }

    // dar de baja una estacion (solo si esta desconectada), devuelve null si se puede o el motivo si no
    public static synchronized String darDeBaja(String id) {
        Estacion ws = estaciones.get(id);
        if (ws == null) return "NO_EXISTE";
        if (ws.getEstado() != EstadoWS.DESCONECTADA) return "ESTA_CONECTADA"; // su monitor sigue hablando con la central
        try {
            BaseDatos.borrarEstacion(id);
        } catch (SQLException e) {
            return "ERROR_BD";
        }
        estaciones.remove(id);
        Mensajes.add("CENTRAL da de baja " + id + " (" + ws.getUbicacion() + ")");
        return null;
    }

    // riegos

    // reservar la ws para un riego, devuelve null si se puede o el motivo si no
    // op = numero de la operacion de riego, se guarda en la estacion para seguir sus pasos
    public static synchronized String reservar(String id, String origen, String op) {
        Estacion ws = estaciones.get(id);
        if (ws == null) return "NO_EXISTE";
        if (ws.getOperario() != null) return "OCUPADA"; // ya la tiene otro
        if (ws.getEstado() != EstadoWS.DISPONIBLE) return ws.getEstado().toString();
        ws.setOperario(origen);
        ws.setOperacion(op);
        ws.setReservadaDesde(System.currentTimeMillis()); // para saber si tarda demasiado en contestar
        return null;
    }

    // el engine ya ha empezado a regar, devuelve quien lo pidio
    public static synchronized String empezarRiego(String id) {
        Estacion ws = estaciones.get(id);
        if (ws == null) return null;
        // nadie le ha pedido regar (por ejemplo la reserva se cancelo porque tardo): que pare
        if (ws.getOperario() == null) {
            KafkaCentral.enviar(KafkaCentral.ordenes(ws.getRegion()), id, "PARAR#" + id);
            return null;
        }

        // si mientras tanto se ha averiado o bloqueado que pare
        if (ws.getEstado() != EstadoWS.DISPONIBLE) {
            KafkaCentral.enviar(KafkaCentral.ordenes(ws.getRegion()), id, "PARAR#" + id);
            Operaciones.paso(ws.getOperacion(), "PARAR enviado: la estacion ya no esta disponible");
            return ws.getOperario();
        }
        ws.setCaudal(0);
        ws.setVolumen(0);
        ws.setReservadaDesde(0); // ya ha contestado, ya no cuenta el tiempo
        cambiarEstado(ws, EstadoWS.REGANDO);
        return ws.getOperario();
    }

    // datos de cada segundo, devuelve quien lo pidio
    public static synchronized String actualizarRiego(String id, double caudal, double volumen) {
        Estacion ws = estaciones.get(id);
        if (ws == null || ws.getOperario() == null) return null;
        ws.setCaudal(caudal);
        ws.setVolumen(volumen);
        return ws.getOperario();
    }

    // fin o rechazo del riego, se libera la ws y devuelve quien lo pidio
    public static synchronized String terminarRiego(String id) {
        Estacion ws = estaciones.get(id);
        if (ws == null || ws.getOperario() == null) return null;
        String operario = ws.getOperario();
        ws.setOperario(null);
        ws.setOperacion(null);
        ws.setReservadaDesde(0);
        ws.setCaudal(0);

        // si estaba regando vuelve a disponible, si tiene fuga o esta desconectada se queda igual
        if (ws.getEstado() == EstadoWS.REGANDO) {
            cambiarEstado(ws, ws.isBloqueada() ? EstadoWS.FUERA_DE_SERVICIO : EstadoWS.DISPONIBLE);
        }
        return operario;
    }

    // reservas que la estacion no ha confirmado en maxMs: se cancelan para que la estacion no se quede
    // reservada para siempre si su engine no contesta. devuelve {id, origen, operacion} de cada una
    public static synchronized List<String[]> cancelarReservasSinRespuesta(long maxMs) {
        List<String[]> canceladas = new ArrayList<>();
        long ahora = System.currentTimeMillis();
        for (Estacion ws : estaciones.values()) {
            if (ws.getOperario() != null && ws.getEstado() != EstadoWS.REGANDO
                    && ws.getReservadaDesde() > 0 && ahora - ws.getReservadaDesde() > maxMs) {
                canceladas.add(new String[] { ws.getId(), ws.getOperario(), ws.getOperacion() });
                ws.setOperario(null);
                ws.setOperacion(null);
                ws.setReservadaDesde(0);
            }
        }
        return canceladas;
    }

    // numero de la operacion de riego que tiene la estacion (null si ninguna)
    public static synchronized String operacion(String id) {
        Estacion ws = estaciones.get(id);
        return ws == null ? null : ws.getOperacion();
    }

    // provincia de una estacion (para saber a que topic mandarle las ordenes)
    public static synchronized String region(String id) {
        Estacion ws = estaciones.get(id);
        return ws == null ? null : ws.getRegion();
    }

    public static Collection<Estacion> todas() {
        return estaciones.values();
    }
}
