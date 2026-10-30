package wm;

// Estado de esta estacion. Lo usan varios hilos a la vez: teclado, monitor,
// consumidor de kafka y riego. Por eso es synchronized
public class Estacion {
    private static String idWS;
    private static boolean averia = false;
    private static boolean bloqueada = false;
    private static boolean sinMonitor = false; // se ha perdido la conexion con el monitor: averiada
    private static Riego riegoActual = null; // null = no esta regando

    public static synchronized void setId(String id) {
        idWS = id;
    }

    public static synchronized boolean hayAveria() {
        return averia;
    }

    // lo llama el consumidor con cada orden que es para nosotros
    public static synchronized void procesarOrden(String[] c) {
        switch (c[0]) {
            case "INICIAR": // INICIAR#id_ws#origen#segundos
                iniciarRiego(c[2], Integer.parseInt(c[3]));
                break;
            case "PARAR": // central nos para por averia
                pararRiego("AVERIA");
                break;
            case "BLOQUEAR":
                bloqueada = true;
                System.out.println(">> Estacion BLOQUEADA por CENTRAL");
                pararRiego("BLOQUEO");
                break;
            case "ACTIVAR":
                bloqueada = false;
                System.out.println(">> Estacion ACTIVADA por CENTRAL");
                break;
            default:
                System.out.println(">> Orden desconocida: " + c[0]);
        }
    }

    private static void iniciarRiego(String origen, int segundos) {
        // comprobar si podemos regar
        String motivo = null;
        if (averia) motivo = "FUGA";
        else if (sinMonitor) motivo = "AVERIADA";
        else if (bloqueada) motivo = "FUERA_DE_SERVICIO";
        else if (riegoActual != null) motivo = "OCUPADA";

        if (motivo != null) {
            System.out.println(">> Riego rechazado: " + motivo);
            KafkaEngine.enviar(KafkaEngine.TELEMETRIA, idWS, "RECHAZADO#" + idWS + "#" + motivo);
            return;
        }
        riegoActual = new Riego(idWS, origen, segundos);
        riegoActual.start();
    }

    // corta el riego en marcha (si hay)
    public static synchronized void pararRiego(String motivo) {
        if (riegoActual != null) riegoActual.parar(motivo);
    }

    // la llama el hilo del teclado. Si hay averia el riego se corta en el acto
    public static synchronized void setAveria(boolean hay) {
        averia = hay;
        System.out.println(hay ? ">> Averia SIMULADA: respondere KO" : ">> Averia resuelta: respondere OK");
        if (hay) pararRiego("AVERIA");
    }

    // la llama App al perder o recuperar el monitor. sin monitor la central no sabe nada de nosotros,
    // asi que se corta el riego (el FIN llega a la central por kafka) y no se riega hasta que vuelva
    public static synchronized void setSinMonitor(boolean caido) {
        if (caido == sinMonitor) return; // solo avisar cuando cambia
        sinMonitor = caido;
        if (caido) {
            System.out.println(">> Sin conexion con el monitor: estacion AVERIADA");
            pararRiego("MONITOR");
        } else {
            System.out.println(">> Monitor recuperado: estacion operativa");
        }
    }

    public static synchronized boolean isSinMonitor() {
        return sinMonitor;
    }

    // la llama el hilo del riego justo antes de terminar
    public static synchronized void riegoTerminado(Riego r) {
        if (riegoActual == r) riegoActual = null;
    }

    public static synchronized String getId() {
        return idWS;
    }

    // Opcion -parar riego-. Devuelve false si no habia riego
    public static synchronized boolean pararManual() {
        if (riegoActual == null) return false;
        riegoActual.parar("MANUAL");
        return true;
    }

    // Para la opcion -ver estado- del menu
    public static synchronized String descripcion() {
        String estado = (riegoActual != null) ? "REGANDO" : "EN REPOSO";
        return idWS + " -> " + estado
                + (averia ? ", CON AVERIA" : "")
                + (bloqueada ? ", BLOQUEADA" : "")
                + (sinMonitor ? ", AVERIADA (sin monitor)" : "");
    }
}