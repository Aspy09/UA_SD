package wm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Scanner;

public class App {
    private static String idWS = null; // lo dice el monitor al conectar
    private static boolean errorMonitorAvisado = false; // para no mandar el mismo error cada 3s mientras reintenta
    private static boolean menuIniciado = false;


    static synchronized void iniciarMenu() {
        mostrarMenu();
        if (menuIniciado) return; // si kafka reasigna mas tarde, solo volver a mostrarlo
        menuIniciado = true;
        Thread menu = new Thread(App::menu);
        menu.setDaemon(true);
        menu.start();
    }

    // Conecta con el monitor y responde a SALUD hasta perder la conexion. no tira excepciones
    private static void atenderMonitor(String ip, int puerto) {
        try (Socket socketMonitor = new Socket()) {
            socketMonitor.connect(new InetSocketAddress(ip, puerto), 3000);
            socketMonitor.setSoTimeout(5000); // el monitor pregunta cada 1ss. Con 5s sin nada se da por perdido
            Protocolo protocolo = new Protocolo(socketMonitor);

            // Saludo e identificacion
            if (!protocolo.enviarENQ() || !protocolo.enviarTrama("CONECTAR")) {
                System.out.println("El Monitor no respondio al saludo");
                return;
            }
            String respuesta = protocolo.recibirTrama();
            if (respuesta == null || !respuesta.startsWith("ID#")) {
                System.out.println("El Monitor rechazo la conexion: " + respuesta);
                return;
            }
            String[] datos = respuesta.split("#");          // ID#id#ubicacion#provincia
            idWS = datos[1];
            String region = datos.length >= 4 ? datos[3] : "ALICANTE"; // provincia de la estacion
            Estacion.setId(idWS);

            System.out.println("Conectado al Monitor. Soy la estacion " + idWS + " (" + region + ")");
            Estacion.setSinMonitor(false);

            KafkaEngine.escucharOrdenes(idWS, region); // empezar a leer las ordenes de su provincia
            errorMonitorAvisado = false; // conectado otra vez, si se vuelve a perder se avisa
            // Responder a cada comprobacion de salud
            String mensaje;
            while ((mensaje = protocolo.recibirTrama()) != null) {
                if (mensaje.startsWith("SALUD#")) {
                    protocolo.enviarTrama((Estacion.hayAveria() ? "KO#" : "OK#") + idWS);
                }
            }
            System.out.println("El Monitor cerro la conexion");
            Estacion.setSinMonitor(true);
            avisarErrorMonitor("el monitor ha cerrado la conexion");

        } catch (IOException e) {
            System.out.println("Monitor no disponible (" + e.getMessage() + ")");
            Estacion.setSinMonitor(true);
            avisarErrorMonitor("no puedo conectar con el monitor (" + e.getMessage() + ")");
        }
    }

    // avisar a la central (solo si ya sabemos quien somos y una vez hasta que se reconecte)
    private static void avisarErrorMonitor(String texto) {
        if (idWS == null || errorMonitorAvisado) return;
        KafkaEngine.error(idWS, texto);
        errorMonitorAvisado = true;
    }

    // Menu de la estacion
    private static void menu() {
        Scanner sc = new Scanner(System.in);
        
        while (sc.hasNextLine()) {
            switch (sc.nextLine().trim()) {
                case "1":
                    pedirRiego(sc);
                    break;
                case "2":
                    if (!Estacion.pararManual()) System.out.println("No hay ningun riego en marcha");
                    break;
                case "3":
                    Estacion.setAveria(!Estacion.hayAveria());
                    break;
                case "4":
                    System.out.println(Estacion.descripcion());
                    break;
                case "5":
                    // salir del menu sin parar la estacion (estacion.sh menu corta la conexion al ver este texto)
                    System.out.println("Has salido del menu (la estacion sigue funcionando)");
                    continue; // no volver a mostrar el menu
                case "":
                    break; // Enter suelto muestra otra vez el menu
                default:
                    System.out.println("Opcion no valida");
            }
            mostrarMenu();
        }
    }

    static void mostrarMenu() {
        System.out.println();
        System.out.println("1) Pedir riego  2) Parar riego  3) Simular/resolver averia  4) Ver estado  5) Salir del menu");
        // println y no print: estacion.sh enseña el menu con docker logs, que no saca una linea hasta que acaba
        System.out.println("Opcion:");
    }

    // Pedir riego desde la propia estacion: origen = el id de la estacion
    private static void pedirRiego(Scanner sc) {
        String id = Estacion.getId();
        if (id == null) {
            System.out.println("Aun no estamos conectados al Monitor");
            return;
        }
        if (Estacion.isSinMonitor()) {
            System.out.println("Estacion AVERIADA (sin conexion con el monitor): no se puede regar");
            return;
        }
        System.out.println("Segundos de riego:");
        if (!sc.hasNextLine()) return;
        try {
            int segundos = Integer.parseInt(sc.nextLine().trim());
            if (segundos <= 0) throw new NumberFormatException();
            KafkaEngine.enviar(KafkaEngine.SOLICITUDES, id, "RIEGO#" + id + "#" + id + "#" + segundos);
        } catch (NumberFormatException e) {
            System.out.println("Hay que poner un numero de segundos mayor que 0");
        }
    }


    //************** MAIN **************

    public static void main(String[] args) {
        // Formato del PDF: <ip_broker:puerto> <ip_monitor:puerto> (el broker se usara con Kafka)
        if (args.length < 2) {
            System.out.println("Uso: java wm.App <ip_broker:puerto> <ip_monitor:puerto>");
            System.exit(1);
        }
        String[] monitor = args[1].split(":");
        if (monitor.length != 2) {
            System.out.println("Error: el monitor debe indicarse como IP:PUERTO");
            System.exit(1);
        }

        // que kafka solo muestre sus errores, no todos sus logs
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
                // si kafka aun no existe o no se encuentra, reintentar en vez de morir
        while (true) {
            try {
                KafkaEngine.iniciar(args[0]);
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

        String ipMonitor = monitor[0];
        int puertoMonitor = Integer.parseInt(monitor[1]);

        // Si perdemos al monitor esperamos 3s y volvemos a intentar
        while (true) {
            atenderMonitor(ipMonitor, puertoMonitor);
            System.out.println("Reintentando conectar con el Monitor en 3 s...");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }
}