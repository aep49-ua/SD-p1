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
    public void registrarEstacion(String ip_wm_central, String puerto_wm_central, String idEstacion, String ubicacion, String puerto_WM_WS_E) {
        Socket skMonitorCentral = null;
        String tramaEnvio = "";
        String respuesta = "";
        String wsCliente = null;

        try {
            // 1. Abrir la conexión con el servidor WM_Central
            int p_wm_central = Integer.parseInt(puerto_wm_central);
            skMonitorCentral = new Socket(ip_wm_central, p_wm_central);
            System.out.println("Conectado con el servidor WM_Central en " + puerto_wm_central + ":" + p_wm_central);

            // 2. Construir la trama con formato: REGISTRO#<ID_ESTACION>#<UBICACION>
            tramaEnvio = "REGISTRO#" + idEstacion + "#" + ubicacion;
            System.out.println("Enviando trama: " + tramaEnvio);

            // 3. Enviar la trama al servidor
            escribeSocket(skMonitorCentral, tramaEnvio);

            // 4. Leer la respuesta del servidor
            respuesta = leeSocket(skMonitorCentral, respuesta);
            System.out.println("Respuesta del servidor: " + respuesta);



            // Una vez recibida la confirmación de WM_Central esperaremos a que nos acepte la conexion por parte de WM_WS_E
           

            // Creamos una conexion con WM_WS_E
            ServerSocket skMonitorEngine = new ServerSocket(Integer.parseInt(puerto_WM_WS_E));
            System.out.println("Escucho el puerto " + puerto_WM_WS_E);
            
            
            for(;;)
            {
            	try {
            		// Esperamos a que acepte la conexion WM_WS_E
                	Socket engineCliente = skMonitorEngine.accept(); 
                    System.out.println("WM_WS_E conectado");
            	
            	
	            	for(;;)
	            	{
	            		// Le mandamos un mensaje de comprobación de estado de salud
	            		escribeSocket(engineCliente, "ESTADO");
	            		
	            		// Si no hemos recibido ningún mensaje de WM_WS_E o el mensaje de respuesta es tipo KO
	            		// enviamos un mensaje de avería al WM_Central
	            		String msg_WM_WS_E = leeSocket(engineCliente, "");
	            		if ("KO".equals(msg_WM_WS_E) || msg_WM_WS_E.isEmpty() || msg_WM_WS_E == null)
	            		{
	            			System.out.println("Avería detectada");
	            			escribeSocket(skMonitorCentral, "AVERIA#" + idEstacion);
	            			
	            			break;
	            		}
	            		
	            		Thread.sleep(1000);
	            	}
	            	
	            	
	            	
	            }catch (Exception e) {
	                System.out.println("Error en la comunicación: " + e.getMessage());
	            } finally {
	                // 5. Cierre limpio de la conexión

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
            // 5. Cierre limpio de la conexión
            try {
                if (skMonitorEngine != null && !skMonitorEngine.isClosed()) {
                	skMonitorEngine.close();
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

        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_M <puerto_WM_WS_E> <ip_WM_Central> <puerto_WM_Central> <id_ws>");
            System.exit(-1);
        }

        /*String host = args[0];
        String puerto = args[1];
        */
        String idEstacion = "";
        String ubicacion = "";
        

        String puerto_WM_WS_E = args[0];
        String ip_wm_central = args[1];
        String puerto_wm_central = args[2];
        Int id_ws = args[3];

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

            //monitor.registrarEstacion(host, puerto, idEstacion, ubicacion);
        
            registrarEstacion(ip_wm_central, puerto_wm_central, idEstacion, ubicacion, puerto_WM_WS_E);

        } catch (Exception e) {
            System.out.println("Error en la lectura de entrada: " + e.getMessage());
        }
    }
}