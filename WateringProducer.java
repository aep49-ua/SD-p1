import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class WateringProducer implements AutoCloseable {

    private KafkaProducer<String, String> producer;

    public WateringProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public WateringProducer() {
        this("localhost:9092");
    }

  
    public void send(String topic, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);

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
        producer.flush();
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println(">>> [WaterManagement] Estación de riego simulando envío de eventos...");

            for (int e = 0; e < 1000; e++) {
                String payload = String.format("METRICS#WS-04#WATERING#8.5#%d", e);

                ProducerRecord<String, String> record = new ProducerRecord<>("wm-telemetria", payload);

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