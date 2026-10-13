import java.io.*;
import java.net.*;
 

public class WM_FO {
	public static void main(String [] args) {
		if(args.length < 3) {
			System.out.println("Uso: java WM_FO <ip_broker> <puerto_broker> <id_operador>");
            System.exit(-1);
		}
		
		String ip_broker = args[0];
		String puerto_broker = args[1];
		String id_operador = args[2];
		
		String fichero = "";
		
		// Configuración del Productor de Kafka para enviar peticiones a la Central
        String bootstrapServers = ipBroker + ":" + puertoBroker;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println(">>> [WM_FO] Operador " + idOperador + " conectado al Broker de Kafka en " + bootstrapServers);

            if (rutaFichero != null) {
                procesarFicheroServicios(producer, idOperador, rutaFichero);
            } else {
                procesarModoInteractivo(producer, idOperador);
            }

        } catch (Exception e) {
            System.err.println("Error en la aplicación WM_FO: " + e.getMessage());
        }
    
	}
	
	
	/**
     * Lee las solicitudes de un fichero, envía la primera, espera su conclusión, 
     * descansa 4 segundos y procesa la siguiente.
     */
    private static void procesarFicheroServicios(KafkaProducer<String, String> producer, String idOperador, String rutaFichero) {
        System.out.println(">>> Leyendo servicios desde el fichero: " + rutaFichero);
        
        try (BufferedReader br = new BufferedReader(new FileReader(rutaFichero))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                linea = linea.trim();
                if (linea.isEmpty() || linea.startsWith("#")) continue; 

                // Formato esperado en la línea del fichero (ej: ID_ESTACION, TIEMPO_SEGUNDOS)
                String[] partes = linea.split(",");
                if (partes.length < 2) continue;

                String idEstacion = partes[0].trim();
                String duracion = partes[1].trim();

                // Enviar la petición de solicitud de suministro
                enviarPeticionRiego(producer, idOperador, idEstacion, duracion);

                // "Enviará el primero a WM_Central y, tras su conclusión (éxito o fracaso), esperará 4 segundos"
                System.out.println(">>> Esperando la conclusión del servicio y 4 segundos antes del siguiente...");
                Thread.sleep(4000); 
            }
        } catch (IOException e) {
            System.err.println("Error al leer el archivo de servicios: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Proceso de lectura de fichero interrumpido.");
        }
    }

    /**
     * Permite solicitar un suministro de forma puntual mediante la consola.
     */
    private static void procesarModoInteractivo(KafkaProducer<String, String> producer, String idOperador) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("\n--- Modo Interactivo Field Operator (" + idOperador + ") ---");
        System.out.println("Escribe el ID de la estación a regar (o 'salir' para terminar):");

        while (true) {
            System.out.print("Estación > ");
            String idEstacion = scanner.nextLine().trim();
            if (idEstacion.equalsIgnoreCase("salir")) break;

            System.out.print("Duración en segundos > ");
            String duracion = scanner.nextLine().trim();

            enviarPeticionRiego(producer, idOperador, idEstacion, duracion);
            System.out.println("Petición enviada. Esperando conclusión...");
            
            try {
                Thread.sleep(4000); 
            } catch (InterruptedException e) {
                break;
            }
        }
        scanner.close();
    }

    /**
     * Envía el mensaje estructurado de solicitud de riego al tópico de Kafka correspondiente.
     */
    private static void enviarPeticionRiego(KafkaProducer<String, String> producer, String idOperador, String idEstacion, String duracion) {
        // Estructura JSON para la petición de riego
        String jsonPayload = String.format(
            "{\"action\": \"REQUEST_WATERING\", \"operator_id\": \"%s\", \"station_id\": \"%s\", \"duration_seconds\": %s}",
            idOperador, idEstacion, duracion
        );

        // Tópico genérico o específico para peticiones a la central
        ProducerRecord<String, String> record = new ProducerRecord<>("water-requests-topic", jsonPayload);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("[Error] No se pudo enviar la solicitud para " + idEstacion + ": " + exception.getMessage());
                } else {
                    System.out.println("[Éxito] Petición enviada a la estación " + idEstacion + " por el operario " + idOperador);
                }
            }
        });
    }
	
	
}