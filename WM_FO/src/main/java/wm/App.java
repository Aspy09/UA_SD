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


    // Modo fichero: una peticion por linea (WS-01#60). Lanza una, espera a que acabe y4s
    static void lanzarFichero(String ruta) {
        List<String> lineas;
        try {
            // ISO_8859_1 nunca falla al leer (aunque el fichero tenga tildes raras) ~sugerencia que me tiro, ni idea~
            lineas = Files.readAllLines(Path.of(ruta), StandardCharsets.ISO_8859_1);
        } catch (IOException e) {
            System.out.println("No se puede leer el fichero " + ruta + ": " + e.getMessage());
            KafkaFO.error(idFO, "no puedo leer el fichero " + ruta);
            return;
        }

        enFichero = true;
        int n = 0;
        for (String linea : lineas) {
            n++;
            linea = linea.trim();
            if (linea.isEmpty()) continue;

            String[] c = linea.split("#");
            if (c.length != 2) {
                System.out.println("Linea " + n + " ignorada (formato ID#segundos): " + linea);
                KafkaFO.error(idFO, "linea " + n + " del fichero mal escrita: " + linea);
                continue;
            }
            String idWS = c[0].trim().toUpperCase();
            String segundos = c[1].trim();
            System.out.println("\n--- Linea " + n + ": " + idWS + " durante " + segundos + " s");

            finalizados.clear(); // descartar avisos viejos
            if (!pedirRiego(idWS, segundos)) continue; // linea mal escrita, entonces no se envia

            try {
                if (!esperarFin(idWS, Integer.parseInt(segundos))) {
                    System.out.println("Sin respuesta de CENTRAL para " + idWS + "; paso a la siguiente");
                }
                Thread.sleep(4000); // 4s entre repes
            } catch (InterruptedException e) {
                break;
            }
        }
        enFichero = false;
        System.out.println("\n--- Fichero terminado");
    }

    // Espera el RESUMEN o DENEGADO de la estacion, como mucho la duracion del riego + 60s
    private static boolean esperarFin(String idWS, int segundos) throws InterruptedException {
        long limite = System.currentTimeMillis() + (segundos + 60) * 1000L;
        while (true) {
            long queda = limite - System.currentTimeMillis();
            if (queda <= 0) return false;
            String ws = finalizados.poll(queda, TimeUnit.MILLISECONDS);
            if (ws == null) return false;         // se acabo el tiempo
            if (ws.equals(idWS)) return true;     // es el fin de nuestra peticion
        }
    }


    public static void main(String[] args) {
        // 1) Argumentos
        if (args.length < 2) {
            System.out.println("Uso: java -jar WM_FO.jar <ip_broker:puerto> <id_operario> [fichero]");
            System.exit(1);
        }
        idFO = args[1];

        // 2) Kafka solo va a mostrar sus errores
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
                // si kafka aun no existe o no se encuentra, reintentar en vez de morir
        while (true) {
            try {
                KafkaFO.iniciar(args[0]);
                break;
            } catch (Exception e) {
                System.out.println("Kafka no disponible (" + args[0] + "). Reintentando en 5 s...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    return;
                }
            }
        }
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
        // Opcional: java -jar WM_FO.jar <broker> <id_operario> <fichero> lanza el fichero al arrancar
        if (args.length >= 3) lanzarFichero(args[2]);
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
            KafkaFO.error(idFO, "estacion no valida '" + idWS + "'");
            return false;
        }
        try {
            int segundos = Integer.parseInt(textoSegundos);
            if (segundos <= 0) throw new NumberFormatException();
            KafkaFO.solicitarRiego(idFO, idWS, segundos);
            return true;
        } catch (NumberFormatException e) {
            System.out.println("Segundos no validos: '" + textoSegundos + "'");
            KafkaFO.error(idFO, "segundos no validos '" + textoSegundos + "' para " + idWS);
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
                finalizados.offer(c[2]);
                if (!enFichero) mostrarMenu();
                break;
            case "RIEGO":    // RIEGO#fo#ws#caudal#volumen
                System.out.println(ws + "Caudal " + c[3] + " L/min - Volumen " + c[4] + " L");
                break;
            case "RESUMEN":    // RESUMEN#fo#ws#volumen#duracion#motivo
                System.out.println(ws + "FIN DEL RIEGO: " + c[3] + " L en " + c[4] + " s (" + c[5] + ")");
                finalizados.offer(c[2]);
                if (!enFichero) mostrarMenu();
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