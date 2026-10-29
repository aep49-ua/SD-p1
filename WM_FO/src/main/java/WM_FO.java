import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class WM_FO {

	private static String id_operador = "";
	private static final Set<String> estacionesActivas = ConcurrentHashMap.newKeySet();
	private static final Map<String, String> estaciones = new TreeMap<>();
	private static volatile String esperandoWS = null;
	private static volatile CountDownLatch conclusion = null;
	private static volatile long ultimaRespuesta = 0;

	public static void main(String [] args) {
		if(args.length < 3) {
			System.out.println("Uso: java WM_FO <ip_broker> <puerto_broker> <id_operador> [fichero_servicios]");
            System.exit(-1);
		}

		String ruta_fichero = "";
		if(args.length > 3) {
			ruta_fichero = args[3];
		}

		String ip_broker = args[0];
		String puerto_broker = args[1];
		String id_operador = args[2];
		WM_FO.id_operador = id_operador;


		// Configuración del Productor de Kafka para enviar peticiones a la Central
        String bootstrapServers = ip_broker + ":" + puerto_broker;

        WateringProducer producer = new WateringProducer(bootstrapServers);

        ListenerKafka consumer = new ListenerKafka(bootstrapServers,
        		Arrays.asList("water-responses-topic", "wm-telemetria", "wm-estaciones"), WM_FO::procesarMensaje, "fo-" + id_operador);


        // Crear un hilo para inicializar el consumidor
        Thread threadConsumer = new Thread(consumer);
    	threadConsumer.setDaemon(true);
    	threadConsumer.start();

    	System.out.println("Operario " + id_operador + " conectado al broker " + bootstrapServers);
    	if (!consumer.esperarListo(20000)) {
    		System.out.println("Aviso: no se ha podido suscribir a Kafka todavía; las respuestas pueden tardar.");
    	}

    	if (!ruta_fichero.isEmpty()) {
    		procesarFicheroServicios(producer, id_operador, ruta_fichero);
    	}
    	procesarModoInteractivo(producer, id_operador);

    	consumer.stop();
    	producer.close();

	}


	private static void procesarMensaje(String topic, String mensaje) {
		String[] partes = mensaje.split("#", 5);

		if (topic.equals("wm-estaciones") && partes[0].equals("ESTACIONES") && partes.length >= 2) {
			synchronized (estaciones) {
				estaciones.clear();
				for (String e : mensaje.substring("ESTACIONES#".length()).split(";")) {
					String[] campos = e.split(":", 3);
					if (campos.length == 3) {
						estaciones.put(campos[0], campos[1] + " - " + campos[2]);
					}
				}
			}

		} else if (topic.equals("water-responses-topic") && partes[0].equals("RESPONSE") && partes.length >= 5) {
			if (!partes[1].equals(id_operador)) {
				return;
			}
			String ws = partes[2];
			String resultado = partes[3];
			ultimaRespuesta = System.currentTimeMillis();
			System.out.println("\n[CENTRAL -> " + id_operador + "] " + ws + ": " + resultado + " - " + partes[4]);

			if (resultado.equals("AUTHORIZED")) {
				estacionesActivas.add(ws);
			}

			if (resultado.equals("DENIED") || resultado.equals("FINISHED")) {
				estacionesActivas.remove(ws);
				if (ws.equals(esperandoWS) && conclusion != null) {
					conclusion.countDown();
				}
			}

		} else if (topic.equals("wm-telemetria") && partes[0].equals("METRICS") && partes.length >= 5) {
			String[] m = mensaje.split("#");
			if (m[2].equals("WATERING") && m.length >= 6 && estacionesActivas.contains(m[1])) {
				System.out.println("   [" + m[1] + "] Regando | caudal " + m[3] + " L/min | acumulado " + m[4] + " L | " + m[5] + " s");
			}
		}
	}

	/**
     * Lee las solicitudes de un fichero, envía la primera, espera su conclusión,
     * descansa 4 segundos y procesa la siguiente.
     */
	private static void procesarFicheroServicios(WateringProducer producer, String idOperador, String rutaFichero) {
        System.out.println("Leyendo servicios en el fichero: " + rutaFichero);

        try (BufferedReader br = new BufferedReader(new FileReader(rutaFichero))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                linea = linea.trim();
                if (linea.isEmpty() || linea.startsWith("#")) continue;

                String[] partes = linea.contains("#") ? linea.split("#") : linea.split(",");
                if (partes.length < 2) continue;

                try {
                    String idEstacion = partes[0].trim();
                    int duracion = Integer.parseInt(partes[1].trim());

                    // Enviar la petición a la central con Kafka
                    esperandoWS = idEstacion;
                    conclusion = new CountDownLatch(1);
                    solicitarSuministro(producer, idOperador, idEstacion, duracion);

                    System.out.println("Esperando conclusión del servicio (" + duracion + "s) y pausa de 4s...");
                    if (!conclusion.await(duracion + 30L, TimeUnit.SECONDS)) {
                        System.out.println("No se ha recibido la conclusión del servicio en " + idEstacion + ". Se continúa.");
                    }
                    esperandoWS = null;
                    Thread.sleep(4000);
                } catch (NumberFormatException e) {
                    System.err.println("Línea ignorada (duración no válida): " + linea);
                }
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    /**
     * Permite solicitar un suministro de forma puntual mediante la consola.
     */
	private static void procesarModoInteractivo(WateringProducer producer, String idOperador) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("\n--- Modo Interactivo Field Operator (" + idOperador + ") ---");
        System.out.println("Introduce las peticiones en formato: <idWS>#<duracion_segundos>");
        System.out.println("Escribe 'lista' para ver las estaciones o 'salir' para terminar.");

        while (true) {
            System.out.print("\nWM_FO> ");
            if (!scanner.hasNextLine()) break;
            String entrada = scanner.nextLine().trim();
            if (entrada.equalsIgnoreCase("salir")) break;

            if (entrada.equalsIgnoreCase("lista")) {
                mostrarEstaciones();
                continue;
            }

            String[] partes = entrada.contains("#") ? entrada.split("#") : entrada.split(",");
            if (partes.length >= 2) {
                try {
                    String idEstacion = partes[0].trim();
                    int duracion = Integer.parseInt(partes[1].trim());

                    solicitarSuministro(producer, idOperador, idEstacion, duracion);
                } catch (NumberFormatException e) {
                    System.out.println("Error: La duración debe ser un número entero.");
                }
            } else if (!entrada.isEmpty()) {
                System.out.println("Formato incorrecto. Utiliza: <idWS>#<duracion>");
            }
        }
        scanner.close();
    }

    private static void mostrarEstaciones() {
        synchronized (estaciones) {
            if (estaciones.isEmpty()) {
                System.out.println("Todavía no se ha recibido el listado de estaciones de CENTRAL.");
                return;
            }
            System.out.println("Estaciones de riego:");
            for (Map.Entry<String, String> e : estaciones.entrySet()) {
                System.out.println("  " + e.getKey() + "  " + e.getValue());
            }
        }
    }


    public static void solicitarSuministro(WateringProducer producer, String idOperador, String idWS, int duracionSegundos)
    {
    	System.out.println("[" + idOperador + "]" + " Enviando solicitud de riego a " + idWS + " (" + duracionSegundos + "s)");
    	estacionesActivas.add(idWS);
    	String mensaje = "REQUEST_WATERING#" + idWS + "#" + idOperador + "#" + duracionSegundos;
    	final long enviado = System.currentTimeMillis();
    	producer.send("water-requests-topic", mensaje);
    	new Thread(() -> {
    		try {
    			Thread.sleep(8000);
    		} catch (InterruptedException e) {
    			return;
    		}
    		if (ultimaRespuesta < enviado) {
    			System.out.println("\n[" + idOperador + "] Imposible conectar con WM_Central: no hay respuesta a la solicitud de " + idWS);
    			estacionesActivas.remove(idWS);
    			if (idWS.equals(esperandoWS) && conclusion != null) {
    				conclusion.countDown();
    			}
    		}
    	}).start();
    }


}
