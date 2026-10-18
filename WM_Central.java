import java.net.*;
import java.util.Scanner;

public class WM_Central {

	/**
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		String puerto = "";
		String ipBroker = "";
		String puertoBroker = "";

		try {
			if (args.length < 3) {
				System.out.println("Uso: java WM_Central <puerto_servidor> <ip_broker> <puerto_broker>");
				System.exit(1);
			}

			puerto = args[0];
			ipBroker = args[1];
			puertoBroker = args[2];
			String bootstrapServers = ipBroker + ":" + puertoBroker;

			WateringProducer producer = new WateringProducer(bootstrapServers);
			WateringConsumer consumer = new WateringConsumer(bootstrapServers, "central-group", producer);
			
			new Thread(consumer).start();

			new Thread(() -> {
				Scanner scanner = new Scanner(System.in);
				while (true) {
					String entrada = scanner.nextLine().trim();
					String[] partes = entrada.split("#");
					String comando = partes[0].toUpperCase();

					if (comando.equals("START") && partes.length >= 3) {
						String idWS = partes[1].trim();
						int duracion = Integer.parseInt(partes[2].trim());
						try {
							EstacionDAO.actualizarEstado(idWS, "REGANDO");
							producer.enviarOrdenStart(idWS, "CENTRAL", duracion);
						} catch (Exception e) { System.err.println(e.getMessage()); }

					} else if (comando.equals("BLOCK") && partes.length >= 2) {
						String idWS = partes[1].trim();
						try {
							EstacionDAO.actualizarEstado(idWS, "OUT_OF_SERVICE");
							producer.send("wm-comandos", "BLOCK#" + idWS);
						} catch (Exception e) { System.err.println(e.getMessage()); }

					} else if (comando.equals("ACTIVATE") && partes.length >= 2) {
						String idWS = partes[1].trim();
						try {
							EstacionDAO.actualizarEstado(idWS, "DISPONIBLE");
							producer.send("wm-comandos", "ACTIVATE#" + idWS);
						} catch (Exception e) { System.err.println(e.getMessage()); }
					}
				}
			}).start();

			ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
			System.out.println("Escucho el puerto " + puerto);

			/*
			* Mantenemos la comunicacion con el cliente
			*/ 	
			for(;;)
			{
				/*
				* Se espera un cliente que quiera conectarse
				*/
				Socket skCliente = skServidor.accept();
				System.out.println("Sirviendo cliente...");

				Thread t = new HiloWM(skCliente);
				t.start();
			}
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
	}
}