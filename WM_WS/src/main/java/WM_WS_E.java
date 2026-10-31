import java.lang.Exception;
import java.net.Socket;
import java.io.*;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.Scanner;

public class WM_WS_E extends Thread {

	private Socket skCliente;

	// id de la estación; lo asigna el monitor en el primer mensaje ESTADO
	private static volatile String idWS = null;
	// true cuando se simula un fallo con la tecla k
	private static volatile boolean ko = false;
	// true cuando Central ha puesto la estación fuera de servicio
	private static volatile boolean bloqueada = false;
	// hay un riego en marcha
	private static volatile boolean regando = false;
	// orden para cortar el riego en curso
	private static volatile boolean detener = false;
	// cómo acaba el riego: COMPLETADO, LEAK o BLOQUEADO
	private static volatile String motivoFin = "COMPLETADO";
	private static WateringProducer producer = null;
	private static String bootstrapServers;
	// para arrancar solo una vez el consumidor de Kafka y el menú
	private static boolean escuchando = false;
	// empieza en true hasta que el monitor se conecta
	private static volatile boolean monitorCaido = true;
	// para saber si Central ha contestado a la petición de riego manual
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


	// simula el riego en un hilo aparte: cada segundo genera caudal y volumen, los muestra y los manda por Kafka
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
				// caudal aleatorio entre 9 y 12 L/min
				double caudal = 9 + rnd.nextDouble() * 3;
				// litros por segundo = caudal / 60
				volumen += caudal / 60.0;

				System.out.printf(Locale.US, "[%s] REGANDO | caudal %.1f L/min | acumulado %.2f L | %d s%n", id, caudal, volumen, segundos);
				producer.send("wm-telemetria", String.format(Locale.US, "METRICS#%s#WATERING#%.1f#%.2f#%d", id, caudal, volumen, segundos));
			}

			// el riego ha terminado, o se ha cortado, o se ha bloqueado: se cierra la electroválvula y se avisa a Central
			banner("RIEGO FINALIZADO (" + motivoFin + ")");
			System.out.println("[" + id + "] Electroválvula CERRADA. Riego finalizado (" + motivoFin + "): "
					+ String.format(Locale.US, "%.2f", volumen) + " L en " + segundos + " s");
			producer.send("wm-telemetria", String.format(Locale.US, "METRICS#%s#FINISHED#%.2f#%d#%s", id, volumen, segundos, motivoFin));
			regando = false;
		}).start();
	}

	// estado que se enseña por pantalla; sin monitor o con fallo cuenta como AVERIADA
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

	// cabecera que separa los mensajes por pantalla
	private static void banner(String texto) {
		System.out.println("\n==================== [" + idWS + "] " + texto + " ====================");
	}

	// corta el riego en curso y deja el motivo con el que acaba
	private static void pararRiego(String motivo) {
		if (regando) {
			motivoFin = motivo;
			detener = true;
		}
	}

	// se llama con cada mensaje de Kafka: órdenes de Central para esta estación y respuestas a los riegos manuales
	private static void procesarMensajeKafka(String topic, String mensaje) {
		String[] partes = mensaje.split("#");
		String miId = idWS;
		if (miId == null || partes.length < 2) {
			return;
		}

		if (topic.equals("wm-comandos")) {
			// las órdenes para otras estaciones se ignoran
			if (!partes[1].trim().equals(miId)) {
				return;
			}
			if (partes[0].equals("START") && partes.length >= 4) {
				// una estación averiada o bloqueada no riega
				if (ko || bloqueada || monitorCaido) {
					System.out.println("[" + miId + "] Orden de riego ignorada: estación averiada o fuera de servicio.");
					return;
				}
				iniciarRiego(partes[2].trim(), Integer.parseInt(partes[3].trim()));

			} else if (partes[0].equals("BLOCK")) {
				// Central bloquea la estación: si estaba regando, el riego acaba como BLOQUEADO
				bloqueada = true;
				banner("FUERA DE SERVICIO");
				System.out.println("[" + miId + "] FUERA DE SERVICIO por orden de CENTRAL.");
				pararRiego("BLOQUEADO");

			} else if (partes[0].equals("ACTIVATE")) {
				// se quita el bloqueo y la estación queda en reposo
				bloqueada = false;
				banner(estadoActual());
				System.out.println("[" + miId + "] ACTIVADA por orden de CENTRAL. Estación en reposo.");
			}

		} else if (topic.equals("water-responses-topic")) {
			// respuesta de Central a un riego manual pedido desde esta estación
			if (partes.length >= 5 && partes[1].equals("MANUAL") && partes[2].equals(miId)) {
				respuestaManual = true;
				System.out.println("[" + miId + "] CENTRAL responde a la petición manual: " + partes[3] + " - " + partes[4]);
			}
		}
	}

	// menú por consola de la estación; corre en su propio hilo
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
					// el riego manual se pide a Central como cualquier otro, con el operario MANUAL
					producer.send("water-requests-topic", "REQUEST_WATERING#" + idWS + "#MANUAL#" + segundos);
					// si en 8 s Central no contesta se avisa por pantalla
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
				// finaliza el riego en curso como completado
				if (regando) {
					pararRiego("COMPLETADO");
				} else {
					System.out.println("No hay ningún riego en curso.");
				}
			} else if (opcion.equals("k")) {
				// k alterna entre fallo simulado y fallo resuelto; si hay un riego en marcha se corta como LEAK
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

	// arranca el consumidor de Kafka y el menú; solo se hace una vez, cuando el monitor dice qué estación es
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

      // el engine simula la estación: se conecta al monitor, recibe ESTADO#id y contesta OK o KO
      public static void main(String[] args) {

        // hacen falta la ip y el puerto del broker y la ip y el puerto del monitor
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
        			// si el monitor no está, el engine lo vuelve a intentar cada 2 s
        			skEngineMonitor = new Socket(ip_wm_ws_m, p_wm_ws_m);
        			System.out.println("Conectado con WM_WS_M en " + ip_wm_ws_m + ":" + puerto_wm_ws_m);
        			WM_WS_E engine = new WM_WS_E(skEngineMonitor);

        			for(;;)
        			{
        				String msg = engine.leeSocket(skEngineMonitor, "");

        				// el monitor cerró la conexión: la estación pasa a AVERIADA, pero el riego en curso termina
        				if(msg == null || msg.isEmpty()) {
        					System.out.println("Conexión con el Monitor WM_WS_M cerrada");
        					monitorCaido = true;
        					banner("AVERIADA (sin monitor)");
        					System.out.println("[" + idWS + "] AVERIADA: sin monitor. El riego en curso termina, pero no se admiten más riegos.");
        					break;
        				}

        				// se valida el LRC de la trama del monitor
        				String recibirDatos = ProtocoloUtils.desempaquetarYValidar(msg);

        				if (recibirDatos != null && recibirDatos.startsWith("ESTADO"))
        				{
        					String[] partes = recibirDatos.split("#");
        					// la primera vez el monitor dice qué estación es y se arrancan el consumidor y el menú
        					if (partes.length >= 2 && !partes[1].equals(idWS)) {
        						idWS = partes[1];
        						System.out.println("Estación asignada: " + idWS);
        						arrancarServicios();
        					}

        					// el monitor ha vuelto: la estación vuelve a estar operativa
        					if (monitorCaido) {
        						monitorCaido = false;
        						banner(estadoActual());
        						System.out.println("[" + idWS + "] Monitor conectado. Estación operativa.");
        					}
        					// se contesta al monitor OK, o KO si hay un fallo simulado
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
