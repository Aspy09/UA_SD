package wm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

// servidor web del panel de la central (viene con java, no hace falta libreria)
//   GET  /            -> la pagina panel.html
//   GET  /api/estado  -> estaciones y mensajes en json (la pagina lo pide cada segundo)
//   POST /api/orden   -> ordenes del punto 11: riego, bloquear, activar
public class PanelWeb {

    public static void iniciar(int puerto) throws IOException {
        HttpServer servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        servidor.createContext("/", PanelWeb::pagina);
        servidor.createContext("/api/estado", PanelWeb::estado);
        servidor.createContext("/api/orden", PanelWeb::orden);
        servidor.start(); // va en sus propios hilos, no bloquea el main
    }

    private static void pagina(HttpExchange ex) throws IOException {
        if (!ex.getRequestURI().getPath().equals("/")) {
            responder(ex, 404, "text/plain", "No encontrado");
            return;
        }
        // panel.html esta dentro del jar (src/main/resources)
        try (InputStream in = PanelWeb.class.getResourceAsStream("/panel.html")) {
            String html = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            responder(ex, 200, "text/html; charset=utf-8", html);
        }
    }

    private static void estado(HttpExchange ex) throws IOException {
        StringBuilder json = new StringBuilder("{\"estaciones\":[");

        // synchronized con Estaciones para leer todas sin que cambien a medias
        synchronized (Estaciones.class) {
            List<Estacion> lista = new ArrayList<>(Estaciones.todas());
            lista.sort(Comparator.comparing(Estacion::getId));
            for (int i = 0; i < lista.size(); i++) {
                Estacion ws = lista.get(i);
                if (i > 0) json.append(",");
                json.append("{\"id\":").append(texto(ws.getId()))
                    .append(",\"ubicacion\":").append(texto(ws.getUbicacion()))
                    .append(",\"region\":").append(texto(ws.getRegion()))
                    .append(",\"estado\":").append(texto(ws.getEstado().toString()))
                    .append(",\"caudal\":").append(ws.getCaudal())
                    .append(",\"volumen\":").append(ws.getVolumen())
                    .append(",\"operario\":").append(texto(ws.getOperario()))
                    .append("}");
            }
        }

        // mensajes separados: actividad normal y alertas
        json.append("],\"mensajes\":");
        lista(json, Mensajes.ultimos());
        json.append(",\"alertas\":");
        lista(json, Mensajes.ultimasAlertas());

        // provincias con el topic de sus ordenes, para las pestañas del panel
        json.append(",\"regiones\":[");
        for (int i = 0; i < KafkaCentral.REGIONES.length; i++) {
            if (i > 0) json.append(",");
            json.append("{\"nombre\":").append(texto(KafkaCentral.REGIONES[i]))
                .append(",\"topic\":").append(texto(KafkaCentral.ordenes(KafkaCentral.REGIONES[i]))).append("}");
        }

        // operaciones de riego con todos sus pasos (trazabilidad)
        json.append("],\"operaciones\":[");
        synchronized (Operaciones.class) {
            List<Operaciones.Operacion> ops = Operaciones.todas();
            for (int i = 0; i < ops.size(); i++) {
                Operaciones.Operacion op = ops.get(i);
                if (i > 0) json.append(",");
                json.append("{\"id\":").append(texto(op.id))
                    .append(",\"ws\":").append(texto(op.ws))
                    .append(",\"origen\":").append(texto(op.origen))
                    .append(",\"segundos\":").append(op.segundos)
                    .append(",\"estado\":").append(texto(op.estado))
                    .append(",\"telemetrias\":").append(op.telemetrias)
                    .append(",\"volumen\":").append(op.volumen)
                    .append(",\"pasos\":");
                lista(json, op.pasos);
                json.append("}");
            }
        }

        // operarios dados de alta
        json.append("],\"operarios\":[");
        try {
            List<String[]> operarios = BaseDatos.listarOperarios();
            for (int i = 0; i < operarios.size(); i++) {
                if (i > 0) json.append(",");
                String[] o = operarios.get(i);
                json.append("{\"id\":").append(texto(o[0]))
                    .append(",\"nombre\":").append(texto(o[1]))
                    .append(",\"region\":").append(texto(o[2])).append("}");
            }
        } catch (Exception e) {
            // si la bd falla el panel sigue sin la lista de operarios
        }
        json.append("]}");

        responder(ex, 200, "application/json; charset=utf-8", json.toString());
    }

    // lista de textos en json: ["a","b"]
    private static void lista(StringBuilder json, List<String> textos) {
        json.append("[");
        for (int i = 0; i < textos.size(); i++) {
            if (i > 0) json.append(",");
            json.append(texto(textos.get(i)));
        }
        json.append("]");
    }

    // /api/orden?accion=riego|bloquear|activar&ws=WS-01|TODAS&segundos=60&region=MADRID
    private static void orden(HttpExchange ex) throws IOException {
        // solo POST, asi abrir el enlace en el navegador no ejecuta nada
        if (!ex.getRequestMethod().equals("POST")) {
            responder(ex, 405, "text/plain", "Usa POST");
            return;
        }
        try {
            Map<String, String> p = parametros(ex.getRequestURI().getRawQuery());
            String accion = p.get("accion");
            String idWS = p.get("ws");
            if (accion == null || idWS == null) {
                responder(ex, 400, "text/plain", "Faltan accion o ws");
                return;
            }

            //1.a que estaciones va la orden
            List<String> ids = new ArrayList<>();
            // con TODAS y una provincia, solo las estaciones de esa provincia
            String region = p.get("region");
            boolean conRegion = region != null && !region.isEmpty() && !region.equals("TODAS");
            if (idWS.equals("TODAS")) {
                for (Estacion ws : Estaciones.todas()) {
                    if (!conRegion || ws.getRegion().equals(region)) ids.add(ws.getId());
                }
            } else {
                ids.add(idWS);
            }

            //2.ejecutarla en cada una
            for (String id : ids) {
                switch (accion) {
                    case "riego":
                        int segundos = Integer.parseInt(p.getOrDefault("segundos", "60"));
                        Riegos.solicitarRiego("CENTRAL", id, segundos); // igual que si lo pide un operario
                        break;
                    case "bloquear":
                        Mensajes.add("CENTRAL bloquea " + id);
                        Estaciones.bloquear(id);
                        break;
                    case "activar":
                        Mensajes.add("CENTRAL activa " + id);
                        Estaciones.activar(id);
                        break;
                    case "baja":
                        // solo se pueden dar de baja las desconectadas; con TODAS se saltan las demas
                        String motivo = Estaciones.darDeBaja(id);
                        if (motivo != null && ids.size() == 1) {
                            responder(ex, 400, "text/plain", "No se puede dar de baja " + id + ": " + motivo);
                            return;
                        }
                        break;
                    default:
                        responder(ex, 400, "text/plain", "Accion desconocida: " + accion);
                        return;
                }
            }
            responder(ex, 200, "text/plain", "OK");
        } catch (Exception e) {
            // siempre contestar algo, si no el navegador se queda esperando
            responder(ex, 400, "text/plain", "Error: " + e.getMessage());
        }
    }

    // "accion=riego&ws=WS-01" -> {accion=riego, ws=WS-01}
    private static Map<String, String> parametros(String query) {
        Map<String, String> p = new HashMap<>();
        if (query == null) return p;
        for (String par : query.split("&")) {
            String[] kv = par.split("=", 2);
            if (kv.length == 2) {
                p.put(kv[0], URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
            }
        }
        return p;
    }

    // texto para json: entre comillas y escapando \ y "
    private static String texto(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static void responder(HttpExchange ex, int codigo, String tipo, String cuerpo) throws IOException {
        byte[] bytes = cuerpo.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", tipo);
        ex.sendResponseHeaders(codigo, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }
}
