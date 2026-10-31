import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class PanelWeb {

    // servidor HTTP del panel; usa el HttpServer que ya trae Java, sin librerías aparte
    public static void iniciar(int puerto) throws IOException {
        HttpServer servidor = HttpServer.create(new InetSocketAddress(puerto), 0);
        // 4 hilos para atender las peticiones del panel
        servidor.setExecutor(Executors.newFixedThreadPool(4));

        // la página en sí; panel.html va dentro del jar como recurso
        servidor.createContext("/", ex -> {
            if (!ex.getRequestURI().getPath().equals("/")) {
                responder(ex, 404, "text/plain; charset=utf-8", "No encontrado");
                return;
            }
            try (InputStream in = PanelWeb.class.getResourceAsStream("/panel.html")) {
                if (in == null) {
                    responder(ex, 500, "text/plain; charset=utf-8", "Falta panel.html en los recursos");
                    return;
                }
                responder(ex, 200, "text/html; charset=utf-8", new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
        });

        // el panel pide aquí cada poco el estado de las estaciones y los mensajes, en JSON
        servidor.createContext("/api/estado", ex -> {
            try {
                responder(ex, 200, "application/json; charset=utf-8", construirJson());
            } catch (Exception e) {
                responder(ex, 500, "text/plain; charset=utf-8", "Error: " + e.getMessage());
            }
        });

        // los botones del panel mandan aquí la orden (START, BLOCK o ACTIVATE) por POST
        servidor.createContext("/api/cmd", ex -> {
            if (!ex.getRequestMethod().equals("POST")) {
                responder(ex, 405, "text/plain; charset=utf-8", "Usa POST");
                return;
            }
            Map<String, String> p = parametros(ex.getRequestURI().getRawQuery());
            String accion = p.getOrDefault("accion", "").toUpperCase();
            String ws = p.getOrDefault("ws", "").trim();
            int dur = 0;
            try {
                dur = Integer.parseInt(p.getOrDefault("dur", "0"));
            } catch (NumberFormatException e) {
                responder(ex, 400, "text/plain; charset=utf-8", "La duración debe ser un número entero");
                return;
            }
            // para regar hace falta una estación y una duración mayor que 0
            if (ws.isEmpty() || (accion.equals("START") && dur <= 0)) {
                responder(ex, 400, "text/plain; charset=utf-8", "Selecciona una estación (y una duración > 0 para regar)");
                return;
            }
            responder(ex, 200, "text/plain; charset=utf-8", WM_Central.ejecutarComando(accion, ws, dur));
        });

        servidor.start();
    }

    // monta el JSON a mano para no meter ninguna librería
    private static String construirJson() throws Exception {
        StringBuilder sb = new StringBuilder("{\"estaciones\":[");
        List<String[]> estaciones = EstacionDAO.listarEstaciones();
        for (int i = 0; i < estaciones.size(); i++) {
            String[] e = estaciones.get(i);
            if (i > 0) sb.append(',');
            sb.append("{\"id\":").append(str(e[0]))
              .append(",\"ubicacion\":").append(str(e[1]))
              .append(",\"estado\":").append(str(e[2]))
              .append(",\"caudal\":").append(e[3])
              .append(",\"volumen\":").append(e[4])
              .append(",\"operario\":").append(str(e[5]))
              .append(",\"ultima\":").append(str(e[6])).append('}');
        }
        sb.append("],\"mensajes\":[");
        List<String> mensajes = EventosCentral.ultimos();
        for (int i = 0; i < mensajes.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(str(mensajes.get(i)));
        }
        return sb.append("]}").toString();
    }

    // escapa un texto para meterlo como string dentro del JSON
    private static String str(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': break;
                default: sb.append(c < 0x20 ? ' ' : c);
            }
        }
        return sb.append('"').toString();
    }

    // saca los parámetros de la URL (accion, ws y dur)
    private static Map<String, String> parametros(String query) {
        Map<String, String> mapa = new HashMap<>();
        if (query == null) return mapa;
        for (String par : query.split("&")) {
            String[] kv = par.split("=", 2);
            if (kv.length == 2) {
                mapa.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
            }
        }
        return mapa;
    }

    // manda la respuesta HTTP; Cache-Control no-store para que el navegador no se quede con un estado antiguo
    private static void responder(HttpExchange ex, int codigo, String tipo, String cuerpo) throws IOException {
        byte[] datos = cuerpo.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", tipo);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(codigo, datos.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(datos);
        }
    }
}
