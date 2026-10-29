package wm;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// mensajes de la central: salen por la terminal y en el panel web
// van en dos listas para que en el panel se vean separados:
//   actividad -> lo normal (registros, riegos, ordenes)
//   alertas   -> lo que hay que mirar (fugas, desconexiones, denegaciones, errores)
public class Mensajes {
    private static final int MAX = 50; // solo guardo los ultimos 50 de cada lista
    private static final LinkedList<String> actividad = new LinkedList<>();
    private static final LinkedList<String> alertas = new LinkedList<>();
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    // mensaje normal
    public static synchronized void add(String texto) {
        guardar(actividad, texto);
        System.out.println("[" + hora() + "] " + texto);
    }

    // mensaje de alerta (en la terminal sale con [ALERTA] delante para verlo rapido)
    public static synchronized void alerta(String texto) {
        guardar(alertas, texto);
        System.out.println("[" + hora() + "] [ALERTA] " + texto);
    }

    private static void guardar(LinkedList<String> lista, String texto) {
        lista.addFirst("[" + hora() + "] " + texto); // el mas nuevo arriba
        if (lista.size() > MAX) lista.removeLast();
    }

    private static String hora() {
        return LocalTime.now().format(HORA);
    }

    // devuelvo copias para que el panel las lea tranquilo mientras otros hilos anaden
    public static synchronized List<String> ultimos() {
        return new ArrayList<>(actividad);
    }

    public static synchronized List<String> ultimasAlertas() {
        return new ArrayList<>(alertas);
    }
}
