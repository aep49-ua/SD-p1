import java.io.*;
import java.net.*;

public class WM_WS_M {

    /*
     * Lee datos del socket utilizando DataInputStream.
     */
    public String leeSocket(Socket p_sk, String p_Datos) {
        try {
            InputStream aux = p_sk.getInputStream();
            DataInputStream flujo = new DataInputStream(aux);
            p_Datos = flujo.readUTF();
        } catch (Exception e) {
            System.out.println("Error al leer del socket: " + e.getMessage());
        }
        return p_Datos;
    }

    /*
     * Escribe datos en el socket utilizando DataOutputStream.
     */
    public void escribeSocket(Socket p_sk, String p_Datos) {
        try {
            OutputStream aux = p_sk.getOutputStream();
            DataOutputStream flujo = new DataOutputStream(aux);
            flujo.writeUTF(p_Datos);
        } catch (Exception e) {
            System.out.println("Error al escribir en el socket: " + e.getMessage());
        }
    }

    /*
     * Inicia el registro de la estación de riego ante WM_Central.
     */
    public void registrarEstacion(String p_host, String p_puerto, String idEstacion, String ubicacion) {
        Socket skCliente = null;
        String tramaEnvio = "";
        String respuesta = "";

        try {
            // 1. Abrir la conexión con el servidor WM_Central
            int puerto = Integer.parseInt(p_puerto);
            skCliente = new Socket(p_host, puerto);
            System.out.println("Conectado con el servidor WM_Central en " + p_host + ":" + puerto);

            // 2. Construir la trama con formato: REGISTRO#<ID_ESTACION>#<UBICACION>
            tramaEnvio = "REGISTRO#" + idEstacion + "#" + ubicacion;
            System.out.println("Enviando trama: " + tramaEnvio);

            // 3. Enviar la trama al servidor
            escribeSocket(skCliente, tramaEnvio);

            // 4. Leer la respuesta del servidor
            respuesta = leeSocket(skCliente, respuesta);
            System.out.println("Respuesta del servidor: " + respuesta);

        } catch (Exception e) {
            System.out.println("Error en la comunicación: " + e.getMessage());
        } finally {
            // 5. Cierre limpio de la conexión
            try {
                if (skCliente != null && !skCliente.isClosed()) {
                    skCliente.close();
                    System.out.println("Conexión cerrada limpiamente.");
                }
            } catch (IOException e) {
                System.out.println("Error al cerrar el socket: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        WM_WS_M monitor = new WM_WS_M();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        if (args.length < 2) {
            System.out.println("Uso: java WM_WS_M <host_servidor> <puerto_servidor> [ID_ESTACION] [UBICACION]");
            System.exit(-1);
        }

        String host = args[0];
        String puerto = args[1];
        String idEstacion = "";
        String ubicacion = "";

        try {
            // Si no se pasan como argumentos opcionales, se piden por teclado
            if (args.length >= 4) {
                idEstacion = args[2];
                ubicacion = args[3];
            } else {
                System.out.print("Introduzca el ID de la estación (ej. WS-04): ");
                idEstacion = br.readLine();
                System.out.print("Introduzca la ubicación (ej. River Park): ");
                ubicacion = br.readLine();
            }

            monitor.registrarEstacion(host, puerto, idEstacion, ubicacion);

        } catch (Exception e) {
            System.out.println("Error en la lectura de entrada: " + e.getMessage());
        }
    }
}