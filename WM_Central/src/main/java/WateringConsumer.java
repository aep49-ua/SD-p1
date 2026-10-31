import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class WateringConsumer implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final WateringProducer producer;
    // para poder parar el hilo desde fuera con stop()
    private volatile boolean running = true;

    public WateringConsumer(String bootstrapServers, String groupId, WateringProducer producer) {
        this.producer = producer;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // solo interesan los mensajes nuevos, no los antiguos que hubiera en el topic
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // tiempos para que Kafka detecte pronto que Central se ha caído
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
        // escucha las peticiones de riego de los operarios y la telemetría de las estaciones
        this.consumer.subscribe(Arrays.asList("water-requests-topic", "wm-telemetria"));
    }

    @Override
    public void run() {
        System.out.println(">>> [KAFKA CONSUMER] Escuchando solicitudes y telemetría de la red...");
        try {
            while (running) {
                // espera hasta 1 s a que lleguen mensajes
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    String mensaje = record.value();

                    try {
                        // según el topic, es una petición de riego o telemetría de una estación
                        if (topic.equals("water-requests-topic")) {
                            procesarSolicitudRiego(mensaje, record.timestamp());
                        } else if (topic.equals("wm-telemetria")) {
                            procesarTelemetria(mensaje);
                        }
                    } catch (Exception e) {
                        System.err.println("[CONSUMER ERROR] Mensaje descartado (" + mensaje + "): " + e);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[CONSUMER ERROR] " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    // valida la petición del operario y, si todo está bien, autoriza el riego
    private void procesarSolicitudRiego(String mensaje, long instante) {
        System.out.println("\n[Central - Petición Recibida]: " + mensaje);

        // Trama esperada: REQUEST_WATERING#WS-01#FO-01#120
        String[] partes = mensaje.split("#");

        if (partes.length >= 4 && partes[0].equals("REQUEST_WATERING")) {
            String idEstacion = partes[1].trim();
            String idOperador = partes[2].trim();
            int duracion = Integer.parseInt(partes[3].trim());

            // si la petición tiene más de 10 s se descarta, por ejemplo si Central estaba caído cuando se envió
            if (System.currentTimeMillis() - instante > 10000) {
                String motivo = "Peticion caducada: CENTRAL no estaba disponible cuando se envio";
                producer.notificarOperario(idOperador, idEstacion, "DENIED", motivo);
                EventosCentral.add("DESCARTADA " + idOperador + " -> " + idEstacion + ": petición caducada");
                System.err.println("SRV: [DESCARTADA] " + motivo + " (" + mensaje + ")");
                return;
            }

            try {
                EventosCentral.add("Petición de riego: " + idOperador + " -> " + idEstacion + " (" + duracion + " s)");
                // primera respuesta al operario: se está comprobando
                producer.notificarOperario(idOperador, idEstacion, "CHECKING", "Comprobando operario y estacion");

                String estadoActual = EstacionDAO.obtenerEstado(idEstacion);

                // se deniega si el operario no existe, la duración no vale o la estación no está disponible
                if (!EstacionDAO.operarioValido(idOperador)) {
                    String motivo = "Operario no registrado en CENTRAL";
                    producer.notificarOperario(idOperador, idEstacion, "DENIED", motivo);
                    EventosCentral.add("DENEGADO " + idOperador + " -> " + idEstacion + ": " + motivo);
                    System.err.println("SRV: [DENEGADO] " + motivo + ": " + idOperador);

                } else if (duracion <= 0) {
                    String motivo = "Duracion no valida";
                    producer.notificarOperario(idOperador, idEstacion, "DENIED", motivo);
                    EventosCentral.add("DENEGADO " + idOperador + " -> " + idEstacion + ": " + motivo);

                } else if ("DISPONIBLE".equalsIgnoreCase(estadoActual)) {
                    // todo correcto: se abre el riego en la BD, se manda la orden START a la estación y se autoriza al operario
                    EstacionDAO.iniciarRiego(idEstacion, idOperador);

                    producer.enviarOrdenStart(idEstacion, idOperador, duracion);
                    producer.notificarOperario(idOperador, idEstacion, "AUTHORIZED", "Riego iniciado con exito");
                    EventosCentral.add(idEstacion + " -> riego iniciado por " + idOperador + " (" + duracion + " s)");

                    System.out.println("SRV: [AUTORIZADO] Riego permitido para " + idEstacion + " por operario " + idOperador);

                } else {
                    String motivo = "Estación no disponible. Estado actual: " + estadoActual;
                    producer.notificarOperario(idOperador, idEstacion, "DENIED", motivo);
                    EventosCentral.add("DENEGADO " + idOperador + " -> " + idEstacion + ": " + estadoActual);

                    System.err.println("SRV: [DENEGADO] Riego rechazado en " + idEstacion + " (" + motivo + ")");
                }

            } catch (Exception e) {
                System.err.println("[Central Error BD] Error al verificar la estación " + idEstacion + ": " + e.getMessage());
            }
        } else {
            System.err.println("[Central Error] Formato de petición inválido: " + mensaje);
        }
    }

    // tramas METRICS que manda la estación mientras riega y cuando termina
    private void procesarTelemetria(String mensaje) {
        String[] partes = mensaje.split("#");

        if (partes.length >= 3 && partes[0].equals("METRICS")) {
            String idEstacion = partes[1].trim();
            String eventoOEstado = partes[2].trim();

            // Trama: METRICS#WS-01#WATERING#caudal#volumen_acumulado#tiempo
            if (eventoOEstado.equals("WATERING") && partes.length >= 6) {
                String caudal = partes[3].trim();
                String volumen = partes[4].trim();
                String tiempo = partes[5].trim();

                System.out.printf("   [MONITOR TELEMETRÍA] Estación: %-6s | Caudal: %-5s L/min | Acumulado: %-6s L | Tiempo: %ss%n",
                        idEstacion, caudal, volumen, tiempo);

                try {
                    EstacionDAO.actualizarTelemetria(idEstacion, Double.parseDouble(caudal), Double.parseDouble(volumen));
                } catch (Exception e) {
                    System.err.println("[Central Error BD]: " + e.getMessage());
                }

            }
            // Trama: METRICS#WS-01#FINISHED#volumen_total#duracion_total
            else if (eventoOEstado.equals("FINISHED") && partes.length >= 5) {
                String volumenTotal = partes[3].trim();
                String duracionTotal = partes[4].trim();

                System.out.println("\n=======================================================");
                System.out.println("   [HISTÓRICO DE RIEGO - REGISTRO FINAL - " + idEstacion + "]");
                System.out.println("   - Estación de Riego: " + idEstacion);
                System.out.println("   - Tiempo total transcurrido: " + duracionTotal + " segundos");
                System.out.println("   - Volumen total suministrado: " + volumenTotal + " Litros");
                System.out.println("=======================================================\n");

                // el motivo con el que acabó el riego decide el estado final que queda en el histórico
                String motivoFin = partes.length >= 6 ? partes[5].trim() : "COMPLETADO";
                String estadoFinal = motivoFin.equals("LEAK") ? "INTERRUMPIDO_LEAK"
                        : motivoFin.equals("BLOQUEADO") ? "BLOQUEADO_CENTRAL" : "COMPLETADO";

                try {
                    double volumen = Double.parseDouble(volumenTotal);
                    int duracion = (int) Math.round(Double.parseDouble(duracionTotal));
                    // se cierra el riego en el histórico
                    String operario = EstacionDAO.finalizarRiego(idEstacion, estadoFinal, volumen, duracion);

                    // la estación vuelve a DISPONIBLE solo si seguía en REGANDO; si hay una fuga o un bloqueo se respeta
                    boolean liberada = EstacionDAO.actualizarEstadoSi(idEstacion, "DISPONIBLE", "REGANDO");
                    System.out.println("[Central] Estación " + idEstacion + (liberada
                            ? " liberada. Estado en BD: DISPONIBLE." : " finaliza el riego (estado sin cambios)."));
                    EventosCentral.add(idEstacion + " -> riego finalizado (" + volumenTotal + " L, " + duracionTotal
                            + " s, " + motivoFin + ")");

                    // se le manda al operario el resumen del riego
                    if (operario != null) {
                        producer.notificarOperario(operario, idEstacion, "FINISHED", "Volumen total: " + volumenTotal
                                + " L | Duracion: " + duracionTotal + " s | Motivo: " + motivoFin);
                    }
                } catch (Exception e) {
                    System.err.println("[Central Error BD]: " + e.getMessage());
                }
            }
        }
    }

    public void stop() {
        this.running = false;
    }
}