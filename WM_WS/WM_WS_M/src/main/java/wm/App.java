package wm;

import java.net.Socket;

public class App {
    public static void main(String[] args) {
        // 1) Argumentos
        if (args.length < 4) {
            System.out.println("Uso: java wm.App <puerto_engine> <ip_central:puerto> <id_ws> <ubicacion>");
            System.exit(1);
        }

        String[] central = args[1].split(":");
        if (central.length != 2) {
            System.out.println("Error: la central debe indicarse como IP:PUERTO");
            System.exit(1);
        }
        String ipCentral = central[0];
        int puertoCentral = Integer.parseInt(central[1]);
        String idWS = args[2];
        String ubicacion = args[3];

        try {
            // 2) Conectar y saludar (ENQ -> ACK)
            Socket socketCentral = new Socket(ipCentral, puertoCentral);
            Protocolo protocolo = new Protocolo(socketCentral);

            if (!protocolo.enviarENQ()) {
                System.out.println("CENTRAL no respondio al saludo");
                socketCentral.close();
                return;
            }
            System.out.println("Conectado a CENTRAL");

            // 3) Registro
            protocolo.enviarTrama("REGISTRO#" + idWS + "#" + ubicacion);
            String respuesta = protocolo.recibirTrama();

            if (respuesta == null || !respuesta.startsWith("OK#")) {
                System.out.println("Registro rechazado: " + respuesta);
                socketCentral.close();
                return;
            }
            System.out.println("Estacion " + idWS + " registrada en CENTRAL");

            // 4) La conexion queda abierta (hsata pulsar enter)
            System.out.println("Pulsa Enter para desconectar...");
            System.in.read();// provisional para que esto siga vivo luego lo cambio
            protocolo.enviarEOT();
            socketCentral.close();

        } catch (Exception e) {
            System.out.println("Error con CENTRAL: " + e.getMessage());
        }
    }
}