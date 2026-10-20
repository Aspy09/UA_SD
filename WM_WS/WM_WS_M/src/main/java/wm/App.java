package wm;

import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
public class App {

    // Datos de la estacion (llegan por argumento)
    private static String ipCentral;
    private static int puertoCentral;
    private static String idWS;
    private static String ubicacion;

    // Conexion con central: null mientras no estemos conectados
    private static Socket socketCentral = null;
    private static Protocolo central = null;

    // Envia un aviso a central y muestra su respuesta
    private static void avisarCentral(Protocolo central, String mensaje) throws Exception {
        if (!central.enviarTrama(mensaje)) throw new Exception("CENTRAL no confirmo el mensaje");
        System.out.println("CENTRAL respondio a " + mensaje + ": " + central.recibirTrama());
    }

    // Conecta con central, saluda y se registra
    // Devuelve true si central nos acepta. Nunca tira excepciones
    private static boolean conectarCentral() {
        try {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(ipCentral, puertoCentral), 3000); // max 3s para conectar
            s.setSoTimeout(5000); // max 5s esperando cada respuesta de central
            Protocolo p = new Protocolo(s);

            if (!p.enviarENQ() || !p.enviarTrama("REGISTRO#" + idWS + "#" + ubicacion)) {
                System.out.println("CENTRAL no respondio al saludo");
                s.close();
                return false;
            }
            String respuesta = p.recibirTrama();
            if (respuesta == null || !respuesta.startsWith("OK#")) {
                System.out.println("Registro rechazado: " + respuesta);
                s.close();
                return false;
            }

            // Ahora con el registro confirmado guardamos la conexion
            socketCentral = s;
            central = p;
            System.out.println("Estacion " + idWS + " registrada en CENTRAL");
            return true;
        } catch (IOException e) {
            System.out.println("CENTRAL no disponible (" + e.getMessage() + ")");
            return false;
        }
    }


    // ************************** MAIN **************************

    public static void main(String[] args) {
        // 1) Argumentos
        if (args.length < 4) {
            System.out.println("Uso: java wm.App <puerto_engine> <ip_central:puerto> <id_ws> <ubicacion>");
            System.exit(1);
        }

        String[] datosCentral = args[1].split(":");
        if (datosCentral.length != 2) {
            System.out.println("Error: la central debe indicarse como IP:PUERTO");
            System.exit(1);
        }
        ipCentral = datosCentral[0];
        puertoCentral = Integer.parseInt(datosCentral[1]);
        idWS = args[2];
        ubicacion = args[3];

        try {
            
            // 2) Conectar y registrarse en central
            if (!conectarCentral()) return;

            // 3) Servidor para el engine
            int puertoEngine = Integer.parseInt(args[0]);
            ServerSocket servidorEngine = new ServerSocket(puertoEngine);
            boolean enAveria = false;

            while (true) {
                System.out.println("Esperando al Engine en el puerto " + puertoEngine + "...");
                Socket socketEngine = servidorEngine.accept();
                socketEngine.setSoTimeout(3000); // si el engine tarda mas de 3segs salta la excepcion
                Protocolo engine = new Protocolo(socketEngine);

                try {
                    // 3.1) Saludo e identificacion del engine
                    if (!engine.esperarENQ() || !"CONECTAR".equals(engine.recibirTrama())) {
                        System.out.println("Conexion rechazada: no es un Engine");
                        socketEngine.close();
                        continue;
                    }
                    engine.enviarTrama("ID#" + idWS + "#" + ubicacion);
                    System.out.println("Engine conectado");

                    // 3.2) Comprobacion de salud cada segundo
                    while (true) {
                        engine.enviarTrama("SALUD#" + idWS);
                        String r = engine.recibirTrama();
                        if (r == null) throw new Exception("el Engine cerro la conexion");

                        boolean ok = r.startsWith("OK#");
                        if (!ok && !enAveria) {
                            avisarCentral(central, "AVERIA#" + idWS);
                            enAveria = true;
                        } else if (ok && enAveria) {
                            avisarCentral(central, "RESUELTO#" + idWS);
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
                    avisarCentral(central, "AVERIA#" + idWS);
                    enAveria = true;
                }
                socketEngine.close();
            }

        } catch (Exception e) {
            System.out.println("Error con CENTRAL: " + e.getMessage());
        }
    }
}