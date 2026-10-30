package wm;

import java.net.*;

//esto seria un hilo por monitor conectado
public class HiloCliente extends Thread {
    private Socket skCliente;
    private String idWS = null; // ID de la estacion de esta conexion (null hasta que se registre)

    public HiloCliente(Socket skCliente) {
        this.skCliente = skCliente;
    }

    public void run() {
        try {
            Protocolo protocolo = new Protocolo(skCliente);
            // el monitor manda un mensaje de vida cada segundo: si en 5s no llega nada
            // (se ha caido o se ha cortado la red) la lectura salta y la ws pasa a DESCONECTADA
            skCliente.setSoTimeout(5000);

            // 1. Saludo inicial: el Monitor manda ENQ y le contestamos ACK
            if (!protocolo.esperarENQ()) {
                System.out.println("Conexion rechazada: no empezo con ENQ");
                skCliente.close();
                return;
            }

            // 2. Conexion persistente: atender peticiones hasta que el Monitor se vaya
            String mensaje;
            while ((mensaje = protocolo.recibirTrama()) != null) {
                // los mensajes de vida llegan cada segundo de cada estacion, no se escriben
                if (!mensaje.startsWith("VIDA#")) System.out.println("Recibido: " + mensaje);
                String respuesta = procesar(mensaje.split("#"));

                // Si tras los reintentos no llega la respuesta, damos la conexion por perdida
                if (!protocolo.enviarTrama(respuesta)) break;
            }
        } catch (SocketTimeoutException e) {
            Mensajes.alerta((idWS != null ? idWS : "Un monitor") + " lleva 5 s sin mandar mensajes de vida");
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }

        // 3. Fin de la conexion (EOT, socket cerrado o error): la WS pasa a DESCONECTADA
        if (idWS != null) {
            Estaciones.desconectar(idWS);
        }
        try {
            skCliente.close();
        } catch (Exception e) {
            // ya estaba cerrado
        }
    }

    // Decide que hacer con cada peticion y devuelve la respuesta para el Monitor
    private String procesar(String[] campos) {
        String codigo = campos[0];

        if (codigo.equals("REGISTRO")) {
            if (campos.length < 3) return "KO#FORMATO_INCORRECTO";
            if (idWS != null) return "KO#" + idWS + "#YA_REGISTRADA";
            // provincia: 4o campo (si no viene, ALICANTE). Solo las que tenemos
            String region = campos.length >= 4 ? campos[3].toUpperCase() : "ALICANTE";
            if (!KafkaCentral.esRegion(region)) return "KO#" + campos[1] + "#REGION_DESCONOCIDA";
            String respuesta = Estaciones.registrar(campos[1], campos[2], region);
            if (respuesta.startsWith("OK#")) idWS = campos[1];
            return respuesta;
        }

        // El resto de peticiones solo se aceptan despues de registrarse y con su propio ID
        if (idWS == null) return "KO#NO_REGISTRADA";
        if (campos.length < 2 || !campos[1].equals(idWS)) return "KO#" + idWS + "#ID_INCORRECTO";

        switch (codigo) {
            case "VIDA": // mensaje de vida, solo hay que contestar
                return "OK#" + idWS;
            case "AVERIA":
                Estaciones.averia(idWS);
                return "OK#" + idWS;
            case "RESUELTO":
                Estaciones.resuelto(idWS);
                return "OK#" + idWS;
            default:
                return "KO#" + idWS + "#CODIGO_DESCONOCIDO";
        }
    }
}
