import java.net.*;
import java.util.List;
import java.util.Scanner;

public class WM_Central {

	public static WateringProducer producer;

	public static String ejecutarComando(String accion, String idWS, int duracion) {
		try {
			if (idWS.equalsIgnoreCase("TODAS")) {
				StringBuilder sb = new StringBuilder();
				for (String[] e : EstacionDAO.listarEstaciones()) {
					if (sb.length() > 0) {
						sb.append(" | ");
					}
					sb.append(ejecutarComando(accion, e[0], duracion));
				}
				return sb.toString();
			}

			String estado = EstacionDAO.obtenerEstado(idWS);

			if (accion.equals("START")) {
				producer.send("water-requests-topic", "REQUEST_WATERING#" + idWS + "#CENTRAL#" + duracion);
				return "Solicitud de riego enviada para " + idWS + " (" + duracion + " s)";

			} else if (accion.equals("BLOCK")) {
				if (estado.equals("DESCONECTADA")) {
					return idWS + " está desconectada: no se puede bloquear";
				}
				EstacionDAO.marcarFueraDeServicio(idWS, true);
				if (!estado.equals("LEAK")) {
					EstacionDAO.actualizarEstado(idWS, "FUERA_DE_SERVICIO");
				}
				producer.send("wm-comandos", "BLOCK#" + idWS);
				EventosCentral.add(idWS + " -> puesta Fuera de Servicio por CENTRAL");
				return idWS + " bloqueada (Fuera de Servicio)";

			} else if (accion.equals("ACTIVATE")) {
				if (estado.equals("LEAK") && EstacionDAO.estaFueraDeServicio(idWS)) {
					EstacionDAO.marcarFueraDeServicio(idWS, false);
					producer.send("wm-comandos", "ACTIVATE#" + idWS);
					EventosCentral.add(idWS + " -> activada por CENTRAL (quedará DISPONIBLE al resolver la fuga)");
					return idWS + " activada: quedará DISPONIBLE al resolver la fuga";
				}
				if (!estado.equals("FUERA_DE_SERVICIO")) {
					return idWS + " no está Fuera de Servicio (estado actual: " + estado + ")";
				}
				EstacionDAO.marcarFueraDeServicio(idWS, false);
				EstacionDAO.actualizarEstado(idWS, "DISPONIBLE");
				producer.send("wm-comandos", "ACTIVATE#" + idWS);
				EventosCentral.add(idWS + " -> activada por CENTRAL");
				return idWS + " activada (DISPONIBLE)";
			}
			return "Orden desconocida: " + accion;
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return "Error: " + e.getMessage();
		}
	}

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
				System.out.println("Uso: java WM_Central <puerto_servidor> <ip_broker> <puerto_broker> [puerto_panel_web]");
				System.exit(1);
			}

			puerto = args[0];
			ipBroker = args[1];
			puertoBroker = args[2];
			String bootstrapServers = ipBroker + ":" + puertoBroker;

			int puertoWeb = 8080;
			if (args.length > 3) {
				puertoWeb = Integer.parseInt(args[3]);
			} else if (System.getenv("PORT") != null) {
				puertoWeb = Integer.parseInt(System.getenv("PORT"));
			}

			DatabaseInitializer.crearBaseDeDatos();
			EstacionDAO.marcarTodasDesconectadas();
			EventosCentral.add("CENTRAL iniciada. Esperando estaciones...");

			producer = new WateringProducer(bootstrapServers);
			WateringConsumer consumer = new WateringConsumer(bootstrapServers, "central-group", producer);

			new Thread(consumer).start();

			PanelWeb.iniciar(puertoWeb);

			Thread publicador = new Thread(() -> {
				while (true) {
					try {
						StringBuilder sb = new StringBuilder("ESTACIONES#");
						List<String[]> estaciones = EstacionDAO.listarEstaciones();
						for (String[] e : estaciones) {
							sb.append(e[0]).append(':').append(e[2]).append(':').append(e[1]).append(';');
						}
						producer.send("wm-estaciones", sb.toString());
						Thread.sleep(3000);
					} catch (InterruptedException ie) {
						return;
					} catch (Exception e) {
						System.err.println("[Central] No se pudo publicar el listado de estaciones: " + e.getMessage());
						try { Thread.sleep(3000); } catch (InterruptedException ie) { return; }
					}
				}
			});
			publicador.setDaemon(true);
			publicador.start();

			Thread vigilante = new Thread(() -> {
				try {
					Thread.sleep(15000);
					while (true) {
						try {
							for (String id : EstacionDAO.riegosAbandonados(8)) {
								String operario = EstacionDAO.finalizarRiego(id, "INTERRUMPIDO_LEAK", EstacionDAO.obtenerVolumen(id), -1);
								EstacionDAO.actualizarEstadoSi(id, "DESCONECTADA", "REGANDO");
								EventosCentral.add(id + " -> riego interrumpido: sin noticias de la estación");
								if (operario != null) {
									producer.notificarOperario(operario, id, "FINISHED", "Riego interrumpido: estacion sin respuesta");
								}
							}
						} catch (Exception e) {
							System.err.println("[Central] Error en el vigilante de riegos: " + e.getMessage());
						}
						Thread.sleep(3000);
					}
				} catch (InterruptedException ie) {
					return;
				}
			});
			vigilante.setDaemon(true);
			vigilante.start();

			new Thread(() -> {
				Scanner scanner = new Scanner(System.in);
				while (scanner.hasNextLine()) {
					String entrada = scanner.nextLine().trim();
					String[] partes = entrada.split("#");
					String comando = partes[0].toUpperCase();

					try {
						if (comando.equals("START") && partes.length >= 3) {
							System.out.println(ejecutarComando("START", partes[1].trim(), Integer.parseInt(partes[2].trim())));

						} else if (comando.equals("BLOCK") && partes.length >= 2) {
							System.out.println(ejecutarComando("BLOCK", partes[1].trim(), 0));

						} else if (comando.equals("ACTIVATE") && partes.length >= 2) {
							System.out.println(ejecutarComando("ACTIVATE", partes[1].trim(), 0));

						} else if (!entrada.isEmpty()) {
							System.out.println("Órdenes: START#<ws|TODAS>#<segundos> | BLOCK#<ws|TODAS> | ACTIVATE#<ws|TODAS>");
						}
					} catch (NumberFormatException e) {
						System.out.println("La duración debe ser un número entero.");
					}
				}
			}).start();

			ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
			System.out.println("Escucho el puerto " + puerto);
			System.out.println("Panel web en http://localhost:" + puertoWeb);

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
