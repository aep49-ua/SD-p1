import java.net.*;
import java.util.List;
import java.util.Scanner;

public class WM_Central {

	// productor de Kafka compartido, lo usan el panel, los hilos de cada estación y la consola
	public static WateringProducer producer;

	// lanza una orden (START, BLOCK o ACTIVATE) sobre una estación o sobre todas; la usan el panel web y la consola de Central
	public static String ejecutarComando(String accion, String idWS, int duracion) {
		try {
			// con TODAS se repite la orden estación por estación y se juntan las respuestas
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

			// el estado actual decide si la orden se puede cumplir
			String estado = EstacionDAO.obtenerEstado(idWS);

			// Central no riega directamente: pide el riego por Kafka como si fuera un operario más
			if (accion.equals("START")) {
				producer.send("water-requests-topic", "REQUEST_WATERING#" + idWS + "#CENTRAL#" + duracion);
				return "Solicitud de riego enviada para " + idWS + " (" + duracion + " s)";

			} else if (accion.equals("BLOCK")) {
				// una estación desconectada no recibiría la orden, así que no se bloquea
				if (estado.equals("DESCONECTADA")) {
					return idWS + " está desconectada: no se puede bloquear";
				}
				// se guarda en la BD que está fuera de servicio
				EstacionDAO.marcarFueraDeServicio(idWS, true);
				// si tiene una fuga se deja en LEAK, que manda más que el bloqueo
				if (!estado.equals("LEAK")) {
					EstacionDAO.actualizarEstado(idWS, "FUERA_DE_SERVICIO");
				}
				// se avisa a la estación por el topic de comandos
				producer.send("wm-comandos", "BLOCK#" + idWS);
				EventosCentral.add(idWS + " -> puesta Fuera de Servicio por CENTRAL");
				return idWS + " bloqueada (Fuera de Servicio)";

			} else if (accion.equals("ACTIVATE")) {
				// con fuga y bloqueada: se quita el bloqueo pero sigue en LEAK hasta que se resuelva la avería
				if (estado.equals("LEAK") && EstacionDAO.estaFueraDeServicio(idWS)) {
					EstacionDAO.marcarFueraDeServicio(idWS, false);
					producer.send("wm-comandos", "ACTIVATE#" + idWS);
					EventosCentral.add(idWS + " -> activada por CENTRAL (quedará DISPONIBLE al resolver la fuga)");
					return idWS + " activada: quedará DISPONIBLE al resolver la fuga";
				}
				// solo se puede activar lo que está fuera de servicio
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
			// hacen falta el puerto, la ip y el puerto del broker; el del panel web es opcional
			if (args.length < 3) {
				System.out.println("Uso: java WM_Central <puerto_servidor> <ip_broker> <puerto_broker> [puerto_panel_web]");
				System.exit(1);
			}

			puerto = args[0];
			ipBroker = args[1];
			puertoBroker = args[2];
			String bootstrapServers = ipBroker + ":" + puertoBroker;

			// el panel va por el 8080 salvo que se cambie por argumento o por la variable PORT
			int puertoWeb = 8080;
			if (args.length > 3) {
				puertoWeb = Integer.parseInt(args[3]);
			} else if (System.getenv("PORT") != null) {
				puertoWeb = Integer.parseInt(System.getenv("PORT"));
			}

			// crea las tablas y los datos iniciales si la BD no existe
			DatabaseInitializer.crearBaseDeDatos();
			// al arrancar no se sabe quién sigue vivo, así que todas empiezan desconectadas hasta que se registren
			EstacionDAO.marcarTodasDesconectadas();
			EventosCentral.add("CENTRAL iniciada. Esperando estaciones...");

			// productor y consumidor de Kafka; el consumidor va en su propio hilo
			producer = new WateringProducer(bootstrapServers);
			WateringConsumer consumer = new WateringConsumer(bootstrapServers, "central-group", producer);

			new Thread(consumer).start();

			// arranca el panel web
			PanelWeb.iniciar(puertoWeb);

			// cada 3 s publica en wm-estaciones el listado de estaciones con su estado
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

			// vigilante de riegos: si una estación riega pero lleva más de 8 s sin dar señales, se cierra el riego como interrumpido y se avisa al operario
			Thread vigilante = new Thread(() -> {
				try {
					// margen al arrancar para que las estaciones tengan tiempo de conectarse
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

			// consola de Central: START#ws#segundos, BLOCK#ws y ACTIVATE#ws, igual que los botones del panel
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

			// socket servidor donde se conectan los monitores de las estaciones
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

				// un hilo por cada estación conectada
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
