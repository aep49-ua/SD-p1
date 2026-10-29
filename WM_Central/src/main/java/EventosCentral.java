import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class EventosCentral {

    private static final int MAX = 100;
    private static final LinkedList<String> mensajes = new LinkedList<>();
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static synchronized void add(String mensaje) {
        mensajes.addFirst("[" + LocalTime.now().format(HORA) + "] " + mensaje);
        while (mensajes.size() > MAX) {
            mensajes.removeLast();
        }
    }

    public static synchronized List<String> ultimos() {
        return new ArrayList<>(mensajes);
    }
}
