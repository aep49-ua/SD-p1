import java.io.*;
import java.net.*;


public class WM_WS_M {

    private Socket skMonitorCentral = null;
    private String ipCentral;
    private String puertoCentral;
    private String idEstacion;
    private String ubicacion;

    private boolean averiaReportada = false;

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

    private String enviarACentral(String datos) {
        for (int intento = 0; intento < 3; intento++) {
            escribeSocket(skMonitorCentral, ProtocoloUtils.empaquetarTrama(datos));

            String ack = leeSocket(skMonitorCentral, "");
            if (ack == null || ack.isEmpty()) {
                return null;
            }
            if (ack.equals(ProtocoloUtils.NACK)) {
                continue;
            }

            String respuesta = leeSocket(skMonitorCentral, "");
            String datosRecibir = ProtocoloUtils.desempaquetarYValidar(respuesta);
            if (datosRecibir == null) {
                System.err.println("Trama corrupta o LRC inválido recibido de WM_Central.");
                escribeSocket(skMonitorCentral, ProtocoloUtils.NACK);
                continue;
            }
            escribeSocket(skMonitorCentral, ProtocoloUtils.ACK);
            return datosRecibir;
        }
        return null;
    }

    private int conectarYRegistrar() {
        try {
            if (skMonitorCentral != null && !skMonitorCentral.isClosed()) {
                skMonitorCentral.close();
            }

            // 1. Abrir la conexión con el servidor WM_Central
            int p_wm_central = Integer.parseInt(puertoCentral);
            skMonitorCentral = new Socket();
            skMonitorCentral.connect(new InetSocketAddress(ipCentral, p_wm_central), 5000);
            skMonitorCentral.setSoTimeout(5000);
            System.out.println("Conectado con el servidor WM_Central en " + ipCentral + ":" + p_wm_central);

            // 2. Construir la trama con formato: REGISTRO#<ID_ESTACION>#<UBICACION>
            String tramaEnvio = "REGISTRO#" + idEstacion + "#" + ubicacion;
            System.out.println("Enviando trama: " + tramaEnvio);

            String datosRecibir = enviarACentral(tramaEnvio);
            if (datosRecibir == null) {
                System.out.println("Sin respuesta válida de WM_Central.");
                return 0;
            }

            System.out.println("Respuesta del servidor: " + datosRecibir);
            if (datosRecibir.startsWith("STATUS#OK")) {
                System.out.println("\n==================== [" + idEstacion + "] REGISTRADA EN WM_CENTRAL ====================");
            }
            return datosRecibir.startsWith("STATUS#OK") ? 1 : -1;

        } catch (Exception e) {
            System.out.println("Imposible conectar con WM_Central: " + e.getMessage());
            return 0;
        }
    }

    private void reconectar() throws InterruptedException {
        int resultado;
        while ((resultado = conectarYRegistrar()) != 1) {
            if (resultado == -1) {
                System.out.println("WM_Central rechaza el registro de " + idEstacion + ". Fin.");
                System.exit(-1);
            }
            System.out.println("Reintentando en 3 segundos...");
            Thread.sleep(3000);
        }
        if (averiaReportada) {
            enviarACentral("AVERIA#" + idEstacion);
        }
    }

    private void reportarCentral(String datos) throws InterruptedException {
        if (enviarACentral(datos) == null) {
            System.out.println("Imposible conectar con WM_Central: conexión perdida. Reconectando...");
            reconectar();
            enviarACentral(datos);
        }
    }

    private void latido() throws InterruptedException {
        reportarCentral("PING#" + idEstacion);
    }

    public void registrarEstacion(String ip_wm_central, String puerto_wm_central, String idEstacion, String ubicacion, String puerto_WM_WS_E) {
        this.ipCentral = ip_wm_central;
        this.puertoCentral = puerto_wm_central;
        this.idEstacion = idEstacion;
        this.ubicacion = ubicacion;

        ServerSocket skMonitorEngine = null;

        try {
            reconectar();

            // Una vez recibida la confirmación de WM_Central esperaremos a que nos acepte la conexion por parte de WM_WS_E

            // Creamos una conexion con WM_WS_E
            skMonitorEngine = new ServerSocket(Integer.parseInt(puerto_WM_WS_E));
            skMonitorEngine.setSoTimeout(5000);
            System.out.println("Escucho el puerto " + puerto_WM_WS_E);


            for(;;)
            {
                Socket engineCliente = null;
                try {
                    // Esperamos a que acepte la conexion WM_WS_E
                    try {
                        engineCliente = skMonitorEngine.accept();
                    } catch (SocketTimeoutException e) {
                        latido();
                        continue;
                    }
                    System.out.println("WM_WS_E conectado");
                    engineCliente.setSoTimeout(3000);

                    int ciclos = 0;
                    for(;;)
                    {
                        // Le mandamos un mensaje de comprobación de estado de salud
                        String datosEnviarE = ProtocoloUtils.empaquetarTrama("ESTADO#" + idEstacion);
                        escribeSocket(engineCliente, datosEnviarE);

                        // Si no hemos recibido ningún mensaje de WM_WS_E o el mensaje de respuesta es tipo KO
                        // enviamos un mensaje de avería al WM_Central
                        String msg_WM_WS_E = leeSocket(engineCliente, "");
                        String recibeDatos = ProtocoloUtils.desempaquetarYValidar(msg_WM_WS_E);

                        if (recibeDatos == null || recibeDatos.isEmpty() || "KO".equals(recibeDatos))
                        {
                            if (!averiaReportada)
                            {
                                System.out.println("\n==================== [" + idEstacion + "] AVERÍA DETECTADA: se notifica a WM_Central ====================");
                                averiaReportada = true;
                                reportarCentral("AVERIA#" + idEstacion);
                            }
                            if (recibeDatos == null || recibeDatos.isEmpty())
                            {
                                break;
                            }
                        }
                        else if (recibeDatos.equals("OK") && averiaReportada)
                        {
                            System.out.println("\n==================== [" + idEstacion + "] AVERÍA RESUELTA: se notifica a WM_Central ====================");
                            averiaReportada = false;
                            reportarCentral("RESUELTA#" + idEstacion);
                        }

                        if (++ciclos % 5 == 0) {
                            latido();
                        }
                        Thread.sleep(1000);
                    }

                } catch (Exception e) {
                    System.out.println("Error en la comunicación: " + e.getMessage());
                } finally {
                    try {
                        if (engineCliente != null && !engineCliente.isClosed()) {
                            engineCliente.close();
                            System.out.println("Conexión cerrada limpiamente.");
                        }
                    } catch (IOException e) {
                        System.out.println("Error al cerrar el socket: " + e.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("Error en la comunicación: " + e.getMessage());
        } finally {
            try {
                if (skMonitorEngine != null && !skMonitorEngine.isClosed()) {
                    skMonitorEngine.close();
                    System.out.println("Conexión cerrada limpiamente.");
                }
                if (skMonitorCentral != null && !skMonitorCentral.isClosed()) {
                    skMonitorCentral.close();
                }
            } catch (IOException e) {
                System.out.println("Error al cerrar el socket: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        WM_WS_M monitor = new WM_WS_M();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_M <puerto_WM_WS_E> <ip_WM_Central> <puerto_WM_Central> <id_ws> [ubicacion]");
            System.exit(-1);
        }

        String idEstacion = "";
        String ubicacion = "";

        String puerto_WM_WS_E = args[0];
        String ip_wm_central = args[1];
        String puerto_wm_central = args[2];
        String id_ws = args[3];

        try {
            idEstacion = id_ws;

            if (args.length >= 5) {
                ubicacion = args[4];
            } else {
                System.out.print("Introduzca la ubicación (ej. River Park): ");
                ubicacion = br.readLine();
            }
            if (ubicacion == null || ubicacion.trim().isEmpty()) {
                System.out.println("Falta la ubicación de la estación.");
                System.exit(-1);
            }

            monitor.registrarEstacion(ip_wm_central, puerto_wm_central, idEstacion, ubicacion, puerto_WM_WS_E);

        } catch (Exception e) {
            System.out.println("Error en la lectura de entrada: " + e.getMessage());
        }
    }
}
