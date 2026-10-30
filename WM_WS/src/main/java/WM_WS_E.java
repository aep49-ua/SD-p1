import java.lang.Exception;
import java.net.Socket;
import java.io.*;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.Scanner;

public class WM_WS_E extends Thread {

	private Socket skCliente;

	private static volatile String idWS = null;
	private static volatile boolean ko = false;
	private static volatile boolean bloqueada = false;
	private static volatile boolean regando = false;
	private static volatile boolean detener = false;
	private static volatile String motivoFin = "COMPLETADO";
	private static WateringProducer producer = null;
	private static String bootstrapServers;
	private static boolean escuchando = false;
	private static volatile boolean monitorCaido = true;
	private static volatile boolean respuestaManual = true;

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


	private static synchronized void iniciarRiego(String operador, int duracion) {
		if (regando) {
			System.out.println("[" + idWS + "] Ya hay un riego en curso.");
			return;
		}
		regando = true;
		detener = false;
		motivoFin = "COMPLETADO";
		final String id = idWS;

		new Thread(() -> {
			Random rnd = new Random();
			double volumen = 0;
			int segundos = 0;
			banner("RIEGO EN CURSO");
			System.out.println("[" + id + "] Electroválvula ABIERTA. Riego de " + duracion + " s solicitado por " + operador);

			while (!detener && segundos < duracion) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					break;
				}
				if (detener) {
					break;
				}
				segundos++;
				double caudal = 9 + rnd.nextDouble() * 3;
				volumen += caudal / 60.0;

				System.out.printf(Locale.US, "[%s] REGANDO | caudal %.1f L/min | acumulado %.2f L | %d s%n", id, caudal, volumen, segundos);
				producer.send("wm-telemetria", String.format(Locale.US, "METRICS#%s#WATERING#%.1f#%.2f#%d", id, caudal, volumen, segundos));
			}

			banner("RIEGO FINALIZADO (" + motivoFin + ")");
			System.out.println("[" + id + "] Electroválvula CERRADA. Riego finalizado (" + motivoFin + "): "
					+ String.format(Locale.US, "%.2f", volumen) + " L en " + segundos + " s");
			producer.send("wm-telemetria", String.format(Locale.US, "METRICS#%s#FINISHED#%.2f#%d#%s", id, volumen, segundos, motivoFin));
			regando = false;
		}).start();
	}

	private static String estadoActual() {
		if (monitorCaido || ko) {
			return "AVERIADA";
		}
		if (bloqueada) {
			return "FUERA DE SERVICIO";
		}
		if (regando) {
			return "REGANDO";
		}
		return "DISPONIBLE";
	}

	private static void banner(String texto) {
		System.out.println("\n==================== [" + idWS + "] " + texto + " ====================");
	}

	private static void pararRiego(String motivo) {
		if (regando) {
			motivoFin = motivo;
			detener = true;
		}
	}

	private static void procesarMensajeKafka(String topic, String mensaje) {
		String[] partes = mensaje.split("#");
		String miId = idWS;
		if (miId == null || partes.length < 2) {
			return;
		}

		if (topic.equals("wm-comandos")) {
			if (!partes[1].trim().equals(miId)) {
				return;
			}
			if (partes[0].equals("START") && partes.length >= 4) {
				if (ko || bloqueada || monitorCaido) {
					System.out.println("[" + miId + "] Orden de riego ignorada: estación averiada o fuera de servicio.");
					return;
				}
				iniciarRiego(partes[2].trim(), Integer.parseInt(partes[3].trim()));

			} else if (partes[0].equals("BLOCK")) {
				bloqueada = true;
				banner("FUERA DE SERVICIO");
				System.out.println("[" + miId + "] FUERA DE SERVICIO por orden de CENTRAL.");
				pararRiego("BLOQUEADO");

			} else if (partes[0].equals("ACTIVATE")) {
				bloqueada = false;
				banner(estadoActual());
				System.out.println("[" + miId + "] ACTIVADA por orden de CENTRAL. Estación en reposo.");
			}

		} else if (topic.equals("water-responses-topic")) {
			if (partes.length >= 5 && partes[1].equals("MANUAL") && partes[2].equals(miId)) {
				respuestaManual = true;
				System.out.println("[" + miId + "] CENTRAL responde a la petición manual: " + partes[3] + " - " + partes[4]);
			}
		}
	}

	private static void menuEstacion() {
		Scanner scanner = new Scanner(System.in);
		System.out.println("\n--- Menú de la estación ---");
		System.out.println("  r <segundos>  regar manualmente (lo autoriza CENTRAL)");
		System.out.println("  s             finalizar el riego en curso");
		System.out.println("  k             simular fallo (KO) / resolverlo");
		System.out.println("  e             ver el estado actual de la estación");
		System.out.println("  q             salir\n");

		while (scanner.hasNextLine()) {
			String[] partes = scanner.nextLine().trim().split("\\s+");
			String opcion = partes[0].toLowerCase();

			if (opcion.equals("r") && partes.length >= 2) {
				try {
					int segundos = Integer.parseInt(partes[1]);
					if (monitorCaido || ko) {
						System.out.println("[" + idWS + "] AVERIADA: no se admiten riegos (monitor caído o fallo en la estación).");
						continue;
					}
					respuestaManual = false;
					producer.send("water-requests-topic", "REQUEST_WATERING#" + idWS + "#MANUAL#" + segundos);
					new Thread(() -> {
						try {
							Thread.sleep(8000);
						} catch (InterruptedException e) {
							return;
						}
						if (!respuestaManual) {
							System.out.println("[" + idWS + "] Imposible conectar con WM_Central: la petición no ha recibido respuesta.");
						}
					}).start();
				} catch (NumberFormatException e) {
					System.out.println("Uso: r <segundos>");
				}
			} else if (opcion.equals("s")) {
				if (regando) {
					pararRiego("COMPLETADO");
				} else {
					System.out.println("No hay ningún riego en curso.");
				}
			} else if (opcion.equals("k")) {
				ko = !ko;
				banner(estadoActual());
				if (ko) {
					System.out.println("[" + idWS + "] FALLO simulado: el monitor recibirá KO.");
					pararRiego("LEAK");
				} else {
					System.out.println("[" + idWS + "] Fallo resuelto: el monitor recibirá OK.");
				}
			} else if (opcion.equals("e")) {
				System.out.println("[" + idWS + "] Estado actual: " + estadoActual());
			} else if (opcion.equals("q")) {
				System.exit(0);
			} else if (!opcion.isEmpty()) {
				System.out.println("Opción no válida. Usa r <segundos>, s, k, e o q.");
			}
		}
	}

	private static synchronized void arrancarServicios() {
		if (escuchando) {
			return;
		}
		escuchando = true;

		ListenerKafka listener = new ListenerKafka(bootstrapServers,
				Arrays.asList("wm-comandos", "water-responses-topic"), WM_WS_E::procesarMensajeKafka);
		Thread threadConsumer = new Thread(listener);
		threadConsumer.setDaemon(true);
		threadConsumer.start();

		Thread threadMenu = new Thread(WM_WS_E::menuEstacion);
		threadMenu.setDaemon(true);
		threadMenu.start();
	}

      public static void main(String[] args) {

        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_E <ip_broker> <puerto_broker> <ip_wm_ws_m> <puerto_wm_ws_m>");
            System.exit(-1);
        }


        String ipBroker = args[0];
        String puertoBroker = args[1];
        String ip_wm_ws_m = args[2];
        String puerto_wm_ws_m = args[3];


        // Configurar e inicializar Productor de Kafka
        bootstrapServers = ipBroker + ":" + puertoBroker;

        Socket skEngineMonitor = null;

        try
        {
        	int p_wm_ws_m = Integer.parseInt(puerto_wm_ws_m);

        	// Inicializar el productor Kafka
        	producer = new WateringProducer(bootstrapServers);
        	System.out.println("Conectado al broker de Kafka: " + bootstrapServers);

        	for(;;)
        	{
        		try
        		{
        			skEngineMonitor = new Socket(ip_wm_ws_m, p_wm_ws_m);
        			System.out.println("Conectado con WM_WS_M en " + ip_wm_ws_m + ":" + puerto_wm_ws_m);
        			WM_WS_E engine = new WM_WS_E(skEngineMonitor);

        			for(;;)
        			{
        				String msg = engine.leeSocket(skEngineMonitor, "");

        				if(msg == null || msg.isEmpty()) {
        					System.out.println("Conexión con el Monitor WM_WS_M cerrada");
        					monitorCaido = true;
        					banner("AVERIADA (sin monitor)");
        					System.out.println("[" + idWS + "] AVERIADA: sin monitor. El riego en curso termina, pero no se admiten más riegos.");
        					break;
        				}

        				String recibirDatos = ProtocoloUtils.desempaquetarYValidar(msg);

        				if (recibirDatos != null && recibirDatos.startsWith("ESTADO"))
        				{
        					String[] partes = recibirDatos.split("#");
        					if (partes.length >= 2 && !partes[1].equals(idWS)) {
        						idWS = partes[1];
        						System.out.println("Estación asignada: " + idWS);
        						arrancarServicios();
        					}

        					if (monitorCaido) {
        						monitorCaido = false;
        						banner(estadoActual());
        						System.out.println("[" + idWS + "] Monitor conectado. Estación operativa.");
        					}
        					String enviarDatos = ProtocoloUtils.empaquetarTrama(ko ? "KO" : "OK");
        					engine.escribeSocket(skEngineMonitor, enviarDatos);
        				}
        			}
        		}
        		catch (IOException e)
        		{
        			System.out.println("Esperando a WM_WS_M en " + ip_wm_ws_m + ":" + puerto_wm_ws_m + " ...");
        		}
        		finally
        		{
        			if (skEngineMonitor != null && !skEngineMonitor.isClosed()) {
        				try {
        					skEngineMonitor.close();
        				} catch (IOException e) {
        					e.printStackTrace();
        				}
        			}
        		}
        		Thread.sleep(2000);
        	}

        } catch (Exception e) {
            System.out.println("Error en WM_WS_E: " + e.getMessage());
        }
    }
}
