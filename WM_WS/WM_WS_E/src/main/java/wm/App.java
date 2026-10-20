package wm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Scanner;

public class App {
    // volatile: cambia el hilo del teclado y lee el hilo principal
    private static volatile boolean averiaSimulada = false;
    private static String idWS = null; // lo dice el monitor al conectar


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
            idWS = respuesta.split("#")[1];
            System.out.println("Conectado al Monitor. Soy la estacion " + idWS
                    + ". Pulsa Enter para simular/resolver una averia.");
            KafkaEngine.escucharOrdenes(idWS); // empzar a leer ordenes
            // Responder a cada comprobacion de salud
            String mensaje;
            while ((mensaje = protocolo.recibirTrama()) != null) {
                if (mensaje.startsWith("SALUD#")) {
                    protocolo.enviarTrama((averiaSimulada ? "KO#" : "OK#") + idWS);
                }
            }
            System.out.println("El Monitor cerro la conexion");

        } catch (IOException e) {
            System.out.println("Monitor no disponible (" + e.getMessage() + ")");
        }
    }

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
        KafkaEngine.iniciar(args[0]);

        // Hilo del teclado: cada Enter activa o desactiva la averia simulada
        Thread teclado = new Thread(() -> {
            Scanner sc = new Scanner(System.in);
            while (sc.hasNextLine()) {
                sc.nextLine();
                averiaSimulada = !averiaSimulada;
                System.out.println(averiaSimulada ? ">> Averia SIMULADA: respondere KO"
                                                  : ">> Averia resuelta: respondere OK");
            }
        });
        teclado.setDaemon(true);
        teclado.start();

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