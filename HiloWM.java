import java.lang.Exception;
import java.net.Socket;
import java.io.*;

public class HiloWM extends Thread {

	private Socket skCliente;
	
	public HiloWM(Socket p_cliente)
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
	
	public int sumar(int p_a, int p_b)
	{
		return p_a+p_b;
	}

	public int multiplicar(int p_a, int p_b)
	{
		return p_a*p_b;
	}

	public int realizarOperacion(String p_Cadena)
	{
		String[] operacion = p_Cadena.split(",");
		int res=0;
		
		System.out.println("SRV: La operacion es: " + operacion[0]);
		if(operacion.length != 1)
		{
			System.out.println("SRV: El operando 1 es " + operacion[1] + " y el operando 2 es " + operacion[2]);
			if(operacion[0].compareTo("suma")==0)
			{
				res = sumar(Integer.parseInt(operacion[1]),Integer.parseInt(operacion[2]));
			}
			else
			{
				if(operacion[0].compareTo("mult")==0)
				{
					res = multiplicar(Integer.parseInt(operacion[1]),Integer.parseInt(operacion[2]));	
				}	
				else
				{
					
					res = -1;
				}
			}
			System.out.println("SRV: El resultado es: " + res);
		}else
		{
			res = -1;
		}	
		return (res);
	}
	
	
	
   	public void run() {
		String Cadena = "";
		String idEstacion = "Desconocida";
		
		try {
			while (true) {
				Cadena = this.leeSocket(skCliente, "");

				if (Cadena == null || Cadena.trim().isEmpty()) {
					System.out.println("SRV: Cliente " + idEstacion + " ha cerrado la conexión.");
					break;
				}

				boolean ver = true;
				String[] campos = Cadena.split("#");

				if (campos.length == 3) {
					if (!campos[0].equals("REGISTRO")) {
						ver = false;
					}

					if (!campos[1].startsWith("WS-")) {
						ver = false;
					} else {
						idEstacion = campos[1]; 
					}

					if (campos[2].trim().isEmpty()) {
						ver = false;
					}
				} else {
					ver = false;
				}

				String Mensaje;

				if (ver) {
					Mensaje = "STATUS#OK#Estacion registrada correctamente";
					System.out.println("SRV: Estación " + idEstacion + " REGISTRADA y en estado DISPONIBLE.");
				} else {
					Mensaje = "STATUS#NO OK#Estacion incorrecta";
				}

				this.escribeSocket(skCliente, Mensaje);
			}
		}
		catch (Exception e) {
			System.out.println("SRV: Estación " + idEstacion + " DESCONECTADA súbitamente: " + e.getMessage());
		}
		finally {
			try {
				if (skCliente != null && !skCliente.isClosed()) {
					skCliente.close();
					System.out.println("SRV: Socket liberado para " + idEstacion);
				}
			} catch (Exception e) {
				System.out.println("SRV: Error al cerrar el socket: " + e.getMessage());
			}
		}
	}
}