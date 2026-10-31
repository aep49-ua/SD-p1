import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class EventosCentral {

    // solo se guardan los últimos 100 mensajes
    private static final int MAX = 100;
    private static final LinkedList<String> mensajes = new LinkedList<>();
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    // el más reciente va primero; es synchronized porque lo llaman varios hilos
    public static synchronized void add(String mensaje) {
        mensajes.addFirst("[" + LocalTime.now().format(HORA) + "] " + mensaje);
        while (mensajes.size() > MAX) {
            mensajes.removeLast();
        }
    }

    // devuelve una copia para que el panel la lea sin problemas
    public static synchronized List<String> ultimos() {
        return new ArrayList<>(mensajes);
    }
}
