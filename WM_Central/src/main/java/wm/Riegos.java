package wm;

import java.sql.SQLException;

// que hacer con cada mensaje de riego que llega por kafka
public class Riegos {

    // lo llama el consumidor con cada mensaje
    public static void procesar(String topic, String mensaje) {
        String[] c = mensaje.split("#");

        if (topic.equals(KafkaCentral.SOLICITUDES)) {
            // RIEGO#origen#id_ws#segundos
            if (c[0].equals("RIEGO") && c.length == 4) {
                solicitarRiego(c[1], c[2], Integer.parseInt(c[3]));
            } else {
                Mensajes.add("Solicitud con formato incorrecto: " + mensaje);
            }
            return;
        }

        // telemetria del engine: CODIGO#id_ws#...
        String idWS = c[1];
        switch (c[0]) {
            case "INICIADO":
                riegoIniciado(idWS);
                break;
            case "RECHAZADO":
                riegoRechazado(idWS, c.length > 2 ? c[2] : "");
                break;
            case "RIEGO":
                datosRiego(idWS, Double.parseDouble(c[2]), Double.parseDouble(c[3]));
                break;
            case "FIN":
                finRiego(idWS, c[2], c[3], c[4]);
                break;
            default:
                Mensajes.add("Telemetria desconocida: " + mensaje);
        }
    }

    // origen puede ser un fo, la propia ws o CENTRAL
    public static void solicitarRiego(String origen, String idWS, int segundos) {
        Mensajes.add(origen + " pide regar " + idWS + " durante " + segundos + "s");
        notificar(origen, "INFO#" + origen + "#" + idWS + "#Validando estacion");

        //1.si es un operario mirar que existe en la bd
        if (origen.startsWith("FO-")) {
            try {
                if (!BaseDatos.existeOperario(origen)) {
                    denegar(origen, idWS, "OPERARIO_NO_EXISTE");
                    return;
                }
            } catch (SQLException e) {
                denegar(origen, idWS, "ERROR_BD");
                return;
            }
        }

        //2.mirar que la ws esta disponible y reservarla
        String motivo = Estaciones.reservar(idWS, origen);
        if (motivo != null) {
            denegar(origen, idWS, motivo);
            return;
        }

        //3.decirle al engine que empiece, se autoriza cuando conteste INICIADO
        KafkaCentral.enviar(KafkaCentral.ORDENES, idWS, "INICIAR#" + idWS + "#" + origen + "#" + segundos);
        notificar(origen, "INFO#" + origen + "#" + idWS + "#Esperando confirmacion de la estacion");
    }

    private static void riegoIniciado(String idWS) {
        String origen = Estaciones.empezarRiego(idWS);
        if (origen == null) return; // nadie pidio este riego
        Mensajes.add(idWS + " empieza a regar (" + origen + ")");
        notificar(origen, "AUTORIZADO#" + origen + "#" + idWS);
    }

    private static void riegoRechazado(String idWS, String motivo) {
        String origen = Estaciones.terminarRiego(idWS);
        if (origen == null) return;
        Mensajes.add(idWS + " ha rechazado el riego: " + motivo);
        denegar(origen, idWS, "RECHAZADO");
    }

    private static void datosRiego(String idWS, double caudal, double volumen) {
        String origen = Estaciones.actualizarRiego(idWS, caudal, volumen);
        if (origen == null) return;
        // se lo reenvio al fo para que vea como va
        notificar(origen, "RIEGO#" + origen + "#" + idWS + "#" + caudal + "#" + volumen);
    }

    private static void finRiego(String idWS, String volumen, String duracion, String motivo) {
        String origen = Estaciones.terminarRiego(idWS);
        if (origen == null) return;
        Mensajes.add(idWS + " ha terminado: " + volumen + " L en " + duracion + "s (" + motivo + ")");
        notificar(origen, "RESUMEN#" + origen + "#" + idWS + "#" + volumen + "#" + duracion + "#" + motivo);
    }

    private static void denegar(String origen, String idWS, String motivo) {
        Mensajes.add("Denegado " + idWS + " a " + origen + ": " + motivo);
        notificar(origen, "DENEGADO#" + origen + "#" + idWS + "#" + motivo);
    }

    // key = origen para que a cada fo le lleguen en orden
    private static void notificar(String origen, String mensaje) {
        KafkaCentral.enviar(KafkaCentral.NOTIFICACIONES, origen, mensaje);
    }
}
