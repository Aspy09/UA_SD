package wm;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Scanner;

public class App {
    private static String idFO;
    // el consumidor deja aca el id de la estacion cuando una peticion termina (RESUMEN o DENEGADO)
    private static final BlockingQueue<String> finalizados = new LinkedBlockingQueue<>();
    private static volatile boolean enFichero = false; // mientras corre el fichero no se meustra el menu

    // id_ws -> "ubicacion: ESTADO", ordenado por id y seguro entre hilos
    private static final Map<String, String> estaciones = new ConcurrentSkipListMap<>();
    public static void main(String[] args) {
        // 1) Argumentos
        if (args.length < 2) {
            System.out.println("Uso: java -jar WM_FO.jar <ip_broker:puerto> <id_operario>");
            System.exit(1);
        }
        idFO = args[1];

        // 2) Kafka solo va a mostrar sus errores
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        KafkaFO.iniciar(args[0]);
        System.out.println("Operario " + idFO + " usando Kafka en " + args[0]);

        // 3) Hilo que escucha kafka, esperamos a que este listo antes de mostrar el menu
        CountDownLatch listo = new CountDownLatch(1);
        new ConsumidorFO(args[0], idFO, listo).start();
        System.out.println("Conectando con Kafka...");
        try {
            if (!listo.await(30, TimeUnit.SECONDS)) {
                System.out.println("Kafka tarda en responder; los avisos pueden llegar tarde");
            }
        } catch (InterruptedException e) {
            return;
        }

        // 4) Menu (hilo principal)
        menu();
    }

    private static void menu() {
        Scanner sc = new Scanner(System.in);
        mostrarMenu();
        while (sc.hasNextLine()) {
            switch (sc.nextLine().trim()) {
                case "1":
                    pedirRiegoManual(sc);
                    break;
                case "2":
                    verEstaciones();
                    break;
                case "":
                    break; // volver a mostrar menu
                default:
                    System.out.println("Opcion no valida");
            }
            mostrarMenu();
        }
    }

    static void mostrarMenu() {
        System.out.println();
        System.out.println("=== Operario " + idFO + " ===");
        System.out.println("1) Pedir riego  2) Ver estaciones");
        System.out.print("Opcion: ");
    }

    private static void pedirRiegoManual(Scanner sc) {
        System.out.print("Estacion (ej. WS-01): ");
        if (!sc.hasNextLine()) return;
        String idWS = sc.nextLine().trim().toUpperCase();

        System.out.print("Segundos de riego: ");
        if (!sc.hasNextLine()) return;
        String segundos = sc.nextLine().trim();

        pedirRiego(idWS, segundos);
    }

    // valida y envia
    static boolean pedirRiego(String idWS, String textoSegundos) {
        if (idWS.isEmpty() || idWS.contains("#")) {
            System.out.println("Estacion no valida: '" + idWS + "'");
            return false;
        }
        try {
            int segundos = Integer.parseInt(textoSegundos);
            if (segundos <= 0) throw new NumberFormatException();
            KafkaFO.solicitarRiego(idFO, idWS, segundos);
            return true;
        } catch (NumberFormatException e) {
            System.out.println("Segundos no validos: '" + textoSegundos + "'");
            return false;
        }
    }

    // ESTADO#id_ws#ubicacion#estado (de wm.estaciones)
    static void actualizarEstacion(String mensaje) {
        String[] c = mensaje.split("#");
        estaciones.put(c[1], c[2] + ": " + c[3]);
    }

    // mensajes de wm.notificaciones: solo importan los de nuestro id
    static void notificacion(String mensaje) {
        String[] c = mensaje.split("#");
        if (!c[1].equals(idFO)) return;

        String ws = "[" + c[2] + "] ";
        switch (c[0]) {
            case "INFO":       // INFO#fo#ws#texto
                System.out.println(ws + c[3]);
                break;
            case "AUTORIZADO": // AUTORIZADO#fo#ws
                System.out.println(ws + "Riego AUTORIZADO");
                break;
            case "DENEGADO":   // DENEGADO#fo#ws#motivo
                System.out.println(ws + "Riego DENEGADO: " + c[3]);
                mostrarMenu();
                break;
            case "RIEGO":    // RIEGO#fo#ws#caudal#volumen
                System.out.println(ws + "Caudal " + c[3] + " L/min - Volumen " + c[4] + " L");
                break;
            case "RESUMEN":    // RESUMEN#fo#ws#volumen#duracion#motivo
                System.out.println(ws + "FIN DEL RIEGO: " + c[3] + " L en " + c[4] + " s (" + c[5] + ")");
                mostrarMenu();
                break;
            default:
                System.out.println(ws + mensaje);
        }
    }

    private static void verEstaciones() {
        if (estaciones.isEmpty()) {
            System.out.println("No se conoce ninguna estacion todavia");
            return;
        }
        for (Map.Entry<String, String> e : estaciones.entrySet()) {
            System.out.println(e.getKey() + " - " + e.getValue());
        }
    }
}