package wm;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Estado de todas las estaciones, compartido por todos los hilos de la Central.
 * Cada HiloCliente lo modifica y (mas adelante) el panel lo lee.
 */
public class Estaciones {
    private static final ConcurrentHashMap<String, Estacion> estaciones = new ConcurrentHashMap<>();

    // REGISTRO: devuelve la respuesta que hay que enviar al Monitor.
    // synchronized: comprobar "ya conectada" y cambiar el estado tiene que ser atomico,
    // si no dos Monitores con el mismo ID podrian registrarse a la vez.
    public static synchronized String registrar(String id, String ubicacion) {
        Estacion ws = estaciones.get(id);
        if (ws == null) {
            ws = new Estacion(id, ubicacion);
            estaciones.put(id, ws);
            System.out.println("[ALTA] Nueva estacion " + id + " en " + ubicacion);
        } else if (ws.getEstado() != EstadoWS.DESCONECTADA) {
            return "KO#" + id + "#YA_CONECTADA";
        }
        ws.setUbicacion(ubicacion);
        ws.setEstado(ws.isBloqueada() ? EstadoWS.FUERA_DE_SERVICIO : EstadoWS.DISPONIBLE);
        System.out.println("[ESTADO] " + ws);
        return "OK#" + id;
    }

    public static synchronized void averia(String id) {
        Estacion ws = estaciones.get(id);
        // TODO paso 5: si estaba REGANDO, enviar PARAR al Engine por Kafka
        ws.setEstado(EstadoWS.FUGA);
        System.out.println("[ESTADO] " + ws);
    }

    public static synchronized void resuelto(String id) {
        Estacion ws = estaciones.get(id);
        ws.setEstado(ws.isBloqueada() ? EstadoWS.FUERA_DE_SERVICIO : EstadoWS.DISPONIBLE);
        System.out.println("[ESTADO] " + ws);
    }

    public static synchronized void desconectar(String id) {
        Estacion ws = estaciones.get(id);
        ws.setEstado(EstadoWS.DESCONECTADA);
        System.out.println("[ESTADO] " + ws);
    }

    public static Collection<Estacion> todas() {
        return estaciones.values();
    }
}
