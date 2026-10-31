import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class WateringProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;

    public WateringProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // tiempos cortos para que, si Kafka no está, el error salga en pocos segundos y no se quede colgado
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");

        this.producer = new KafkaProducer<>(props);
    }

    public void enviarOrdenStart(String idEstacion, String idOperador, int duracion) {
        String orden = String.format("START#%s#%s#%d", idEstacion, idOperador, duracion);
        send("wm-comandos", orden);
    }

    public void notificarOperario(String idOperador, String idEstacion, String resultado, String motivo) {
        String mensaje = String.format("RESPONSE#%s#%s#%s#%s", idOperador, idEstacion, resultado, motivo);
        send("water-responses-topic", mensaje);
    }

    // envía el mensaje y hace flush; si falla se muestra el error por pantalla
    public void send(String topic, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("[PRODUCER ERROR] Imposible conectar con Kafka. No se pudo publicar en " + topic + ": " + exception.getMessage());
                } else if (!topic.equals("wm-telemetria") && !topic.equals("wm-estaciones")) {
                    System.out.println("[KAFKA PUB] -> Tópico: " + metadata.topic() + " | Trama: " + payload);
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
}