package wm;

import java.net.Socket;
import java.util.Scanner;

public class App {
    // volatile: cambia el hilo del teclado y lee el hilo principal
    private static volatile boolean averiaSimulada = false;

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

        try (Socket socketMonitor = new Socket(monitor[0], Integer.parseInt(monitor[1]))) {
            Protocolo protocolo = new Protocolo(socketMonitor);

            // Saludo e identificacion
            if (!protocolo.enviarENQ()) {
                System.out.println("El Monitor no respondio al saludo");
                return;
            }
            protocolo.enviarTrama("CONECTAR");
            String respuesta = protocolo.recibirTrama();
            if (respuesta == null || !respuesta.startsWith("ID#")) {
                System.out.println("El Monitor rechazo la conexion: " + respuesta);
                return;
            }
            String idWS = respuesta.split("#")[1];
            System.out.println("Soy la estacion " + idWS + ". Pulsa Enter para simular/resolver una averia.");

            // Responder a cada comprobacion de salud
            String mensaje;
            while ((mensaje = protocolo.recibirTrama()) != null) {
                if (mensaje.startsWith("SALUD#")) {
                    protocolo.enviarTrama((averiaSimulada ? "KO#" : "OK#") + idWS);
                }
            }
            System.out.println("El Monitor cerro la conexion");

        } catch (Exception e) {
            System.out.println("Error con el Monitor: " + e.getMessage());
        }
    }
}