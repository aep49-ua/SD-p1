public class ProtocoloUtils {

    public static final char STX = (char) 0x02; // Start of Text
    public static final char ETX = (char) 0x03; // End of Text
    public static final String ACK = "\u0006";  // Acknowledgment
    public static final String NACK = "\u0015"; // Negative Acknowledgment

    /**
     * Calcula el LRC mediante XOR byte a byte del mensaje (DATA)
     */
    public static char calcularLRC(String datos) {
        int lrc = 0;
        for (int i = 0; i < datos.length(); i++) {
            lrc ^= datos.charAt(i);
        }
        return (char) lrc;
    }

    /**
     * Construye la trama en formato <STX><DATA><ETX><LRC>
     */
    public static String empaquetarTrama(String datos) {
        char lrc = calcularLRC(datos);
        return STX + datos + ETX + lrc;
    }

    /**
     * Extrae el contenido DATA de una trama <STX><DATA><ETX><LRC> y valida su LRC
     */
    public static String desempaquetarYValidar(String tramaCompleta) {
        if (tramaCompleta == null || tramaCompleta.length() < 3) {
            return null;
        }

        // Verificar que empieza por STX
        if (tramaCompleta.charAt(0) != STX) {
            return null;
        }

        int posETX = tramaCompleta.indexOf(ETX);
        if (posETX == -1 || posETX + 1 >= tramaCompleta.length()) {
            return null;
        }

        String datos = tramaCompleta.substring(1, posETX);
        char lrcRecibido = tramaCompleta.charAt(posETX + 1);

        // Validar LRC
        if (calcularLRC(datos) == lrcRecibido) {
            return datos; // Devuelve solo el <DATA>
        }

        return null; // LRC incorrecto (corrupto)
    }
}