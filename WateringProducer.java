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

    
    public void send(String topic, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("[PRODUCER ERROR] Error al publicar en " + topic + ": " + exception.getMessage());
                } else {
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