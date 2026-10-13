import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class WateringProducer {
    public static void main(String[] args) {

    	Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println(">>> [WaterManagement] Estación de riego simulando envío de eventos...");

            for (int e = 0; e < 1000; e++) {
                String jsonPayload = String.format("{\"station_id\": \"WS-04\", \"number\": %d, \"status\": \"WATERING\", \"flow_l_min\": 8.5}", e);

                ProducerRecord<String, String> record = new ProducerRecord<>("numtest", jsonPayload);

                // Envío asíncrono con callback de confirmación
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Error de entrega en Kafka: " + exception.getMessage());
                        } else {
                            System.out.println("Evento enviado a tópico: " + metadata.topic() + " [Partición: " + metadata.partition() + "]");
                        }
                    }
                });

                Thread.sleep(5000);
            }

            producer.flush();
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Productor de estación interrumpido.");
        }
    }
}
