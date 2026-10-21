import java.io.*;
import java.net.*;
 

public class WM_FO {
	public static void main(String [] args) {
		if(args.length < 3) {
			System.out.println("Uso: java WM_FO <ip_broker> <puerto_broker> <id_operador>");
            System.exit(-1);
		}
		
		String ruta_fichero = "";
		if(args.length > 3) {
			ruta_fichero = args[3];
		}
		
		String ip_broker = args[0];
		String puerto_broker = args[1];
		String id_operador = args[2];
		ruta_fichero
		
		
		// Configuración del Productor de Kafka para enviar peticiones a la Central
        String bootstrapServers = ip_broker + ":" + puerto_broker;
        
        WateringProducer producer = new WateringProducer(bootstrapServers);
        WateringConsumer consumer = new WateringConsumer(bootstrapServers);
        
        
        // Crear un hilo para inicializar el consumidor
        Thread threadConsumer = new Thread(consumer);
    	threadConsumer.setDaemon(true);
    	threadConsumer.start();
        
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

                String idEstacion = partes[0].trim();
                int duracion = Integer.parseInt(partes[1].trim());

                // Enviar la petición a la central con Kafka
                solicitarSuministro(producer, idOperador, idEstacion, duracion);

                // Esperar a que el riego concluya (duración) + 4 segundos 
                int tiempoEsperaTotal = duracion + 4;
                System.out.println("Esperando conclusión del servicio (" + duracion + "s) y pausa de 4s...");
                Thread.sleep(tiempoEsperaTotal * 1000L);
            }
        } catch (IOException | InterruptedException | NumberFormatException e) {
            System.err.println("Error: " + e.getMessage());
        } 
    }

    /**
     * Permite solicitar un suministro de forma puntual mediante la consola.
     */
	private static void procesarModoInteractivo(WateringProducer producer, String idOperador) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("\n--- Modo Interactivo Field Operator (" + idOperador + ") ---");
        System.out.println("Introduce las peticiones en formato: <idWS>#<duracion_segundos> (o 'salir' para terminar)");

        while (true) {
            System.out.print("\nWM_FO> ");
            String entrada = scanner.nextLine().trim();
            if (entrada.equalsIgnoreCase("salir")) break;

            String[] partes = entrada.contains("#") ? entrada.split("#") : entrada.split(",");
            if (partes.length >= 2) {
                try {
                    String idEstacion = partes[0].trim();
                    int duracion = Integer.parseInt(partes[1].trim());

                    solicitarSuministro(producer, idOperador, idEstacion, duracion);
                } catch (NumberFormatException e) {
                    System.out.println("Error: La duración debe ser un número entero.");
                }
            } else {
                System.out.println("Formato incorrecto. Utiliza: <idWS>#<duracion>");
            }
        }
        scanner.close();
    }

    
    public void solicitarSuministro(WateringProducer producer)
    {
    	System.out.println("[FO" + idOpereador + "]" + " Enviando solicitud de riego a " + idWS + " (" + duracionSegundos + "s)");
    	String mensaje = "START#" + idWS + "#" + idOperador + "#" + duracionSegundos;
    	producer.enviarOrdenStart(idWS, idOperador, mensaje);
    }
	
	
}