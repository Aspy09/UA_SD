package wm;

import java.net.ServerSocket;
import java.net.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
public class App {

    // Datos de la estacion (llegan por argumento)
    private static String ipCentral;
    private static int puertoCentral;
    private static String idWS;
    private static String ubicacion;
    private static long ultimoIntento = 0; // para no reintentar mas de una vez cada 5s

    // enAveria = lo que le pasa al engine
    // averiaAvisada = lo que cree central
    private static boolean enAveria = false;
    private static boolean averiaAvisada = false;

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

        ultimoIntento = System.currentTimeMillis();

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

            averiaAvisada = false; // central nos acaba de registrar como DISPONIBLE

            return true;
        } catch (IOException e) {
            System.out.println("CENTRAL no disponible (" + e.getMessage() + ")");
            return false;
        }
    }

    // Central nunca escribe sin que preguntemos nosotros, asi que si al leer
    // nos pasa -1 o un error, es porque se cerró
    private static boolean centralViva() {
        try {
            socketCentral.setSoTimeout(1); // mirar sin quedarnos esperando
            return socketCentral.getInputStream().read() != -1;
        } catch (SocketTimeoutException e) {
            return true; // no habia nada que leer: la conexion sigue abierta
        } catch (IOException e) {
            return false;
        } finally {
            try {
                socketCentral.setSoTimeout(5000); // volver al timeout normal
            } catch (IOException e) {
                // el socket ya esta cerrado
            }
        }
    }

    // Cierra la conexion con central y deja los atributos a null
    private static void cerrarCentral() {
        try {
            if (socketCentral != null) socketCentral.close();
        } catch (IOException e) {
            // ya estaba cerrado
        }
        socketCentral = null;
        central = null;
    }

    // Envia un aviso a central y muestra su respuesta. Nunca lanza excepciones.
    private static boolean avisarCentral(String mensaje) {
        try {
            if (!central.enviarTrama(mensaje)) return false;
            String respuesta = central.recibirTrama();
            System.out.println("CENTRAL respondio a " + mensaje + ": " + respuesta);
            return respuesta != null;
        } catch (IOException e) {
            return false;
        }
    }

    // Se llama una vez por segundo. Comprueba la conexion, reconecta si hace falta
    // y avisa a central si el estado de averia ccambió. Nunca lanza excepciones.
    private static void mantenerCentral() {
        // 1) Si estabamos conectados, comprueba que central sigue ahi
        if (central != null && !centralViva()) {
            System.out.println("Conexion con CENTRAL perdida. Reintentando cada 5s ...");
            cerrarCentral();
        }

        // 2) Si no estamos conectados,reintenta (max cada 5 s)
        if (central == null) {
            if (System.currentTimeMillis() - ultimoIntento < 5000) return;
            if (!conectarCentral()) return;
        }

        // 3) Avisar solo si lo que sabe central no coincide con la realidad
        if (enAveria != averiaAvisada) {
            String mensaje = (enAveria ? "AVERIA#" : "RESUELTO#") + idWS;
            if (avisarCentral(mensaje)) {
                averiaAvisada = enAveria;
            } else {
                System.out.println("No se pudo avisar a CENTRAL de " + mensaje);
                cerrarCentral();
            }
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