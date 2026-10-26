package wm;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// mensajes de la central: salen por la terminal y en el panel web
public class Mensajes {
    private static final int MAX = 50; // solo guardo los ultimos 50
    private static final LinkedList<String> lista = new LinkedList<>();
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static synchronized void add(String texto) {
        String linea = "[" + LocalTime.now().format(HORA) + "] " + texto;
        System.out.println(linea);
        lista.addFirst(linea); // el mas nuevo arriba
        if (lista.size() > MAX) lista.removeLast();
    }

    // devuelvo una copia para que el panel la lea tranquilo mientras otros hilos anaden
    public static synchronized List<String> ultimos() {
        return new ArrayList<>(lista);
    }
}
