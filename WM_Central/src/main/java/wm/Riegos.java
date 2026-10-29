package wm;

import java.sql.SQLException;

// que hacer con cada mensaje de riego que llega por kafka
// cada riego tiene su numero de operacion (OP-0007) y se apuntan sus pasos en Operaciones
public class Riegos {

    // lo llama el consumidor con cada mensaje
    public static void procesar(String topic, String mensaje) {
        String[] c = mensaje.split("#");

        // error de otro modulo: ERROR#quien#texto -> a las alertas del panel
        if (topic.equals(KafkaCentral.ERRORES)) {
            String[] e = mensaje.split("#", 3); // el texto puede llevar # dentro
            if (e.length == 3) Mensajes.alerta("Error en " + e[1] + ": " + e[2]);
            else Mensajes.alerta("Error de otro modulo: " + mensaje);
            return;
        }

        if (topic.equals(KafkaCentral.SOLICITUDES)) {
            // RIEGO#origen#id_ws#segundos
            if (c[0].equals("RIEGO") && c.length == 4) {
                solicitarRiego(c[1], c[2], Integer.parseInt(c[3]));
            } else {
                Mensajes.alerta("Solicitud con formato incorrecto: " + mensaje);
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
                Mensajes.alerta("Telemetria desconocida: " + mensaje);
        }
    }

    // origen puede ser un fo, la propia ws o CENTRAL
    public static void solicitarRiego(String origen, String idWS, int segundos) {
        // nueva operacion: desde aqui todos los pasos de este riego llevan su numero
        String op = Operaciones.nueva(idWS, origen, segundos);
        Mensajes.add(op + " " + origen + " pide regar " + idWS + " durante " + segundos + "s");
        notificar(origen, "INFO#" + origen + "#" + idWS + "#Validando estacion");
        Operaciones.paso(op, "INFO a " + origen + ": validando estacion");

        //1.si es un operario mirar que existe en la bd
        if (origen.startsWith("FO-")) {
            try {
                if (!BaseDatos.existeOperario(origen)) {
                    denegar(op, origen, idWS, "OPERARIO_NO_EXISTE");
                    return;
                }
                Operaciones.paso(op, "El operario " + origen + " existe en la bd");
            } catch (SQLException e) {
                denegar(op, origen, idWS, "ERROR_BD");
                return;
            }
        }

        //2.mirar que la ws esta disponible y reservarla
        String motivo = Estaciones.reservar(idWS, origen, op);
        if (motivo != null) {
            denegar(op, origen, idWS, motivo);
            return;
        }
        Operaciones.paso(op, idWS + " esta disponible, queda reservada");

        //3.decirle al engine que empiece, se autoriza cuando conteste INICIADO
        String topic = KafkaCentral.ordenes(Estaciones.region(idWS));
        KafkaCentral.enviar(topic, idWS, "INICIAR#" + idWS + "#" + origen + "#" + segundos);
        Operaciones.paso(op, "INICIAR enviado por " + topic);
        notificar(origen, "INFO#" + origen + "#" + idWS + "#Esperando confirmacion de la estacion");
        Operaciones.paso(op, "INFO a " + origen + ": esperando confirmacion de la estacion");
    }

    private static void riegoIniciado(String idWS) {
        String op = Estaciones.operacion(idWS); // antes de cambiar nada, saber de que operacion es
        String origen = Estaciones.empezarRiego(idWS);
        if (origen == null) return; // nadie pidio este riego
        Operaciones.paso(op, "INICIADO: " + idWS + " ha abierto la valvula");
        Mensajes.add(op + " " + idWS + " empieza a regar (" + origen + ")");
        notificar(origen, "AUTORIZADO#" + origen + "#" + idWS);
        Operaciones.paso(op, "AUTORIZADO enviado a " + origen);
    }

    private static void riegoRechazado(String idWS, String motivo) {
        String op = Estaciones.operacion(idWS);
        String origen = Estaciones.terminarRiego(idWS);
        if (origen == null) return;
        Operaciones.paso(op, "RECHAZADO por la estacion: " + motivo);
        Mensajes.alerta(op + " " + idWS + " ha rechazado el riego: " + motivo);
        denegar(op, origen, idWS, "RECHAZADO");
    }

    private static void datosRiego(String idWS, double caudal, double volumen) {
        String op = Estaciones.operacion(idWS);
        String origen = Estaciones.actualizarRiego(idWS, caudal, volumen);
        if (origen == null) return;
        Operaciones.telemetria(op, volumen); // llega cada segundo, solo se cuenta
        // se lo reenvio al fo para que vea como va
        notificar(origen, "RIEGO#" + origen + "#" + idWS + "#" + caudal + "#" + volumen);
    }

    private static void finRiego(String idWS, String volumen, String duracion, String motivo) {
        String op = Estaciones.operacion(idWS);
        String origen = Estaciones.terminarRiego(idWS);
        if (origen == null) return;
        Operaciones.paso(op, "FIN: " + volumen + " L en " + duracion + "s (" + motivo + ")");
        Mensajes.add(op + " " + idWS + " ha terminado: " + volumen + " L en " + duracion + "s (" + motivo + ")");
        notificar(origen, "RESUMEN#" + origen + "#" + idWS + "#" + volumen + "#" + duracion + "#" + motivo);
        Operaciones.paso(op, "RESUMEN enviado a " + origen);
        // si acaba por tiempo o a mano ha ido bien, si no se ha cortado (averia, bloqueo...)
        boolean bien = motivo.equals("TIEMPO") || motivo.equals("MANUAL");
        Operaciones.terminar(op, bien ? "TERMINADA" : "CORTADA (" + motivo + ")");
    }

    private static void denegar(String op, String origen, String idWS, String motivo) {
        Mensajes.alerta(op + " Denegado " + idWS + " a " + origen + ": " + motivo);
        notificar(origen, "DENEGADO#" + origen + "#" + idWS + "#" + motivo);
        Operaciones.paso(op, "DENEGADO a " + origen + ": " + motivo);
        Operaciones.terminar(op, "DENEGADA (" + motivo + ")");
    }

    // key = origen para que a cada fo le lleguen en orden
    private static void notificar(String origen, String mensaje) {
        KafkaCentral.enviar(KafkaCentral.NOTIFICACIONES, origen, mensaje);
    }
}
