import java.io.*;
import java.net.*;


public class WM_WS_M {

    // conexión del monitor con Central
    private Socket skMonitorCentral = null;
    private String ipCentral;
    private String puertoCentral;
    private String idEstacion;
    private String ubicacion;

    // para avisar de la avería a Central una sola vez hasta que se resuelva
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

    // manda una trama a Central y devuelve su respuesta; reintenta hasta 3 veces si hay NACK o llega corrupta, y devuelve null si se pierde la conexión
    private String enviarACentral(String datos) {
        for (int intento = 0; intento < 3; intento++) {
            escribeSocket(skMonitorCentral, ProtocoloUtils.empaquetarTrama(datos));

            String ack = leeSocket(skMonitorCentral, "");
            if (ack == null || ack.isEmpty()) {
                return null;
            }
            // Central no entendió la trama (LRC mal), se vuelve a enviar
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
            // la respuesta llegó bien, se confirma con ACK
            escribeSocket(skMonitorCentral, ProtocoloUtils.ACK);
            return datosRecibir;
        }
        return null;
    }

    // abre la conexión con Central y registra la estación; devuelve 1 si Central acepta, -1 si rechaza y 0 si no se pudo conectar
    private int conectarYRegistrar() {
        try {
            if (skMonitorCentral != null && !skMonitorCentral.isClosed()) {
                skMonitorCentral.close();
            }

            // 1. Abrir la conexión con el servidor WM_Central
            int p_wm_central = Integer.parseInt(puertoCentral);
            skMonitorCentral = new Socket();
            // 5 s de espera para conectar y para leer, para que no se quede colgado si Central no está
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

    // reintenta cada 3 s hasta conseguir registrarse; si Central rechaza el registro, termina
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
        // si había una avería sin resolver se vuelve a avisar después de reconectar
        if (averiaReportada) {
            enviarACentral("AVERIA#" + idEstacion);
        }
    }

    // envía un mensaje a Central; si falla, reconecta y lo vuelve a enviar
    private void reportarCentral(String datos) throws InterruptedException {
        if (enviarACentral(datos) == null) {
            System.out.println("Imposible conectar con WM_Central: conexión perdida. Reconectando...");
            reconectar();
            enviarACentral(datos);
        }
    }

    // ping a Central para que sepa que la estación sigue viva
    private void latido() throws InterruptedException {
        reportarCentral("PING#" + idEstacion);
    }

    // registra la estación en Central y después atiende al engine: le pregunta su estado cada segundo y avisa a Central de las averías y de su resolución
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
            // cada 5 s sin que el engine se conecte se manda un ping a Central
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
                    // si el engine no contesta en 3 s se considera caído
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

                        // sin respuesta o con KO del engine hay avería
                        if (recibeDatos == null || recibeDatos.isEmpty() || "KO".equals(recibeDatos))
                        {
                            if (!averiaReportada)
                            {
                                System.out.println("\n==================== [" + idEstacion + "] AVERÍA DETECTADA: se notifica a WM_Central ====================");
                                averiaReportada = true;
                                reportarCentral("AVERIA#" + idEstacion);
                            }
                            // si el engine ha desaparecido se sale de este bucle y se vuelve a esperar a que se conecte
                            if (recibeDatos == null || recibeDatos.isEmpty())
                            {
                                break;
                            }
                        }
                        // el engine vuelve a contestar OK después de una avería: se avisa de que está resuelta
                        else if (recibeDatos.equals("OK") && averiaReportada)
                        {
                            System.out.println("\n==================== [" + idEstacion + "] AVERÍA RESUELTA: se notifica a WM_Central ====================");
                            averiaReportada = false;
                            reportarCentral("RESUELTA#" + idEstacion);
                        }

                        // cada 5 ciclos (unos 5 s) un ping a Central
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

    // monitor de la estación: se registra en Central y hace de intermediario con el engine
    public static void main(String[] args) {
        WM_WS_M monitor = new WM_WS_M();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        // hacen falta el puerto del engine, la ip y el puerto de Central y el id de la estación; la ubicación es opcional
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

            // si la ubicación no viene por argumento se pregunta por teclado
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
