package wm;

import java.net.ServerSocket;
import java.net.Socket;
public class App {

    // Envia un aviso a central y muestra su respuesta
    private static void avisarCentral(Protocolo central, String mensaje) throws Exception {
        if (!central.enviarTrama(mensaje)) throw new Exception("CENTRAL no confirmo el mensaje");
        System.out.println("CENTRAL respondio a " + mensaje + ": " + central.recibirTrama());
    }

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

            // 4) Servidor para el engine
            int puertoEngine = Integer.parseInt(args[0]);
            ServerSocket servidorEngine = new ServerSocket(puertoEngine);
            boolean enAveria = false;

            while (true) {
                System.out.println("Esperando al Engine en el puerto " + puertoEngine + "...");
                Socket socketEngine = servidorEngine.accept();
                socketEngine.setSoTimeout(3000); // si el engine tarda mas de 3segs salta la excepcion
                Protocolo engine = new Protocolo(socketEngine);

                try {
                    // 4.1) Saludo e identificacion del engine
                    if (!engine.esperarENQ() || !"CONECTAR".equals(engine.recibirTrama())) {
                        System.out.println("Conexion rechazada: no es un Engine");
                        socketEngine.close();
                        continue;
                    }
                    engine.enviarTrama("ID#" + idWS + "#" + ubicacion);
                    System.out.println("Engine conectado");

                    // 4.2) Comprobacion de salud cada segundo
                    while (true) {
                        engine.enviarTrama("SALUD#" + idWS);
                        String r = engine.recibirTrama();
                        if (r == null) throw new Exception("el Engine cerro la conexion");

                        boolean ok = r.startsWith("OK#");
                        if (!ok && !enAveria) {
                            avisarCentral(protocolo, "AVERIA#" + idWS);
                            enAveria = true;
                        } else if (ok && enAveria) {
                            avisarCentral(protocolo, "RESUELTO#" + idWS);
                            enAveria = false;
                        }
                        Thread.sleep(1000);
                    }
                } catch (java.net.SocketTimeoutException | java.io.EOFException e) {
                    System.out.println("Engine sin respuesta");
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("Engine")) {
                        System.out.println("Engine perdido: " + e.getMessage());
                    } else {
                        throw e; // problema con central. Lo gestiona el catch de afuera
                    }
                }

                // Si llegamos aca es porque el Engine fallo. Avisamos y esperamos a otro
                if (!enAveria) {
                    avisarCentral(protocolo, "AVERIA#" + idWS);
                    enAveria = true;
                }
                socketEngine.close();
            }

        } catch (Exception e) {
            System.out.println("Error con CENTRAL: " + e.getMessage());
        }
    }
}