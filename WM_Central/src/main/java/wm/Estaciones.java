package wm;

import java.sql.SQLException;
import java.util.Collection;
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
        Mensajes.add(ws.toString());
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
    public static synchronized String registrar(String id, String ubicacion) {
        Estacion ws = estaciones.get(id);
        if (ws == null) {
            ws = new Estacion(id, ubicacion);
            estaciones.put(id, ws);
            Mensajes.add("Nueva estacion " + id + " en " + ubicacion);
        } else if (ws.getEstado() != EstadoWS.DESCONECTADA) {
            return "KO#" + id + "#YA_CONECTADA";
        }
        ws.setUbicacion(ubicacion);
        guardar(ws);
        cambiarEstado(ws, ws.isBloqueada() ? EstadoWS.FUERA_DE_SERVICIO : EstadoWS.DISPONIBLE);
        return "OK#" + id;
    }

    public static synchronized void averia(String id) {
        Estacion ws = estaciones.get(id);
        // si estaba regando hay que parar el riego
        if (ws.getEstado() == EstadoWS.REGANDO) {
            KafkaCentral.enviar(KafkaCentral.ORDENES, id, "PARAR#" + id);
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
            Mensajes.add("Error guardando " + ws.getId() + " en la bd: " + e.getMessage());
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
        KafkaCentral.enviar(KafkaCentral.ORDENES, id, "BLOQUEAR#" + id);
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
        KafkaCentral.enviar(KafkaCentral.ORDENES, id, "ACTIVAR#" + id);
        if (ws.getEstado() == EstadoWS.FUERA_DE_SERVICIO) {
            cambiarEstado(ws, EstadoWS.DISPONIBLE);
        }
        return true;
    }

    // riegos

    // reservar la ws para un riego, devuelve null si se puede o el motivo si no
    public static synchronized String reservar(String id, String origen) {
        Estacion ws = estaciones.get(id);
        if (ws == null) return "NO_EXISTE";
        if (ws.getOperario() != null) return "OCUPADA"; // ya la tiene otro
        if (ws.getEstado() != EstadoWS.DISPONIBLE) return ws.getEstado().toString();
        ws.setOperario(origen);
        return null;
    }

    // el engine ya ha empezado a regar, devuelve quien lo pidio
    public static synchronized String empezarRiego(String id) {
        Estacion ws = estaciones.get(id);
        if (ws == null || ws.getOperario() == null) return null;

        // si mientras tanto se ha averiado o bloqueado que pare
        if (ws.getEstado() != EstadoWS.DISPONIBLE) {
            KafkaCentral.enviar(KafkaCentral.ORDENES, id, "PARAR#" + id);
            return ws.getOperario();
        }
        ws.setCaudal(0);
        ws.setVolumen(0);
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
        ws.setCaudal(0);

        // si estaba regando vuelve a disponible, si tiene fuga o esta desconectada se queda igual
        if (ws.getEstado() == EstadoWS.REGANDO) {
            cambiarEstado(ws, ws.isBloqueada() ? EstadoWS.FUERA_DE_SERVICIO : EstadoWS.DISPONIBLE);
        }
        return operario;
    }

    public static Collection<Estacion> todas() {
        return estaciones.values();
    }
}
