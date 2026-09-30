import java.lang.Exception;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;

public class WM_WS_E extends Thread {

	private Socket skCliente;
	
	public WM_WS_E(Socket p_cliente)
	{
		this.skCliente = p_cliente;
	}
	
	/*
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 0 si se cierra fichero o -1 si hay error.
	*/
	public String leeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = new String();
			p_Datos = flujo.readUTF();
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return p_Datos;
	}

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
	public void escribeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			OutputStream aux = p_sk.getOutputStream();
			DataOutputStream flujo= new DataOutputStream( aux );
			flujo.writeUTF(p_Datos);      
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		return;
	}
	
	
	
	




      public static void main(String[] args) {
        //WM_WS_E monitor = new WM_WS_E();
        //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_E <ip_broker> <puerto_broker> <ip_wm_ws_m> <puerto_wm_ws_m>");
            System.exit(-1);
        }


        String ipBroker = args[0];
        String puertoBroker = args[1];
        String ip_wm_ws_m = args[2];
        String puerto_wm_ws_m = args[3];


        // Se queda a la espera o envia cada 1 segundos si tiene respuesta el monitor?
 

        // 1. Abrir la conexión con el servidor WM_WS_E
        int puertoEngine = Integer.parseInt(p_Engine);
        wsCliente = new Socket(engineHost, puertoEngine);
        System.out.println("Conectado con el servidor WM_WS_E en " + p_host + ":" + puerto);

        // 2. Enviar la respuesta al servidor
        escribeSocket(wsCliente, respuesta);

        // 3. Cerrar conexion
        try {
            if(wsCliente != null && !wsCliente.isClosed()){
                wsCliente.close();
                System.out.println("Conexión cerrada limpiamente.");

            }
        } catch (IOException e) {
            System.out.println("Error al cerrar el socket: " + e.getMessage());



       
    }
}