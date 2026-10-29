package wm;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// trazabilidad de los riegos: cada riego tiene un numero de operacion (OP-0001...)
// y se apuntan todos sus pasos con la hora: solicitud, validacion, INICIAR, INICIADO, FIN, RESUMEN...
// asi en el panel se puede ver la historia completa de cada riego
public class Operaciones {
    private static final int MAX = 30; // solo guardo las ultimas 30 operaciones
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final LinkedList<Operacion> lista = new LinkedList<>();
    private static int contador = 0; // vuelve a empezar si se reinicia la central

    // los datos de una operacion (un riego)
    public static class Operacion {
        String id, ws, origen, estado = "EN CURSO";
        int segundos;
        int telemetrias = 0;   // cuantos mensajes de telemetria han llegado
        double volumen = 0;    // ultimo volumen que mando el engine
        List<String> pasos = new ArrayList<>();
    }

    // nueva operacion cuando alguien pide un riego, devuelve su numero
    public static synchronized String nueva(String ws, String origen, int segundos) {
        contador++;
        Operacion op = new Operacion();
        op.id = String.format("OP-%04d", contador); // OP-0001, OP-0002...
        op.ws = ws;
        op.origen = origen;
        op.segundos = segundos;
        lista.addFirst(op); // la mas nueva arriba
        if (lista.size() > MAX) lista.removeLast();
        paso(op.id, origen + " pide regar " + ws + " durante " + segundos + "s");
        return op.id;
    }

    // apuntar un paso de la operacion
    public static synchronized void paso(String id, String texto) {
        Operacion op = buscar(id);
        if (op != null) op.pasos.add("[" + LocalTime.now().format(HORA) + "] " + texto);
    }

    // la telemetria llega cada segundo, no la apunto como paso, solo cuento y guardo el volumen
    public static synchronized void telemetria(String id, double volumen) {
        Operacion op = buscar(id);
        if (op == null) return;
        if (op.telemetrias == 0) paso(id, "Llega la primera telemetria del engine"); // la primera si
        op.telemetrias++;
        op.volumen = volumen;
    }

    // cerrar la operacion: TERMINADA, DENEGADA, CORTADA...
    public static synchronized void terminar(String id, String estado) {
        Operacion op = buscar(id);
        if (op != null) op.estado = estado;
    }

    private static Operacion buscar(String id) {
        if (id == null) return null;
        for (Operacion op : lista) {
            if (op.id.equals(id)) return op;
        }
        return null;
    }

    // las ultimas operaciones (el panel las lee dentro de un synchronized(Operaciones.class))
    public static List<Operacion> todas() {
        return lista;
    }
}
