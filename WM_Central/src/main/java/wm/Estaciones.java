package wm;

import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Estado de todas las estaciones, compartido por todos los hilos de la Central.
 * Cada HiloCliente lo modifica y (mas adelante) el panel lo lee.
 */
public class Estaciones {
    private static final ConcurrentHashMap<String, Estacion> estaciones = new ConcurrentHashMap<>();

    // al arrancar: meter en el mapa las estaciones de la bd (desconectadas)
    public static synchronized void cargarDesdeBD() throws SQLException {
        for (Estacion ws : BaseDatos.cargarEstaciones()) {
            estaciones.put(ws.getId(), ws);
            System.out.println("[BD] " + ws);
        }
        System.out.println("[BD] " + estaciones.size() + " estaciones cargadas");
    }

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

        // guardar en la bd, si falla la estacion sigue funcionando igual
        try {
            BaseDatos.guardarEstacion(ws);
        } catch (SQLException e) {
            System.out.println("[BD] Error guardando " + id + ": " + e.getMessage());
        }
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
