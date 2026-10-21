package wm;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// el engine con kafka: enviar telemetria y solicitudes
public class KafkaEngine {
    // nombres de los topics
    public static final String SOLICITUDES = "wm.solicitudes";
    public static final String ORDENES = "wm.ordenes";
    public static final String TELEMETRIA = "wm.telemetria";
    private static boolean escuchando = false;
    private static String broker;
    private static KafkaProducer<String, String> productor;

    public static void iniciar(String brokerKafka) {
        broker = brokerKafka;

        // los mensajes son texto (clave y valor)
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // si kafka esta caido, no quedarse bloqueado mas de 5s al enviar
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        productor = new KafkaProducer<>(props);
    }

    // key = id de la estacion, asi todos sus mensajes llegan en orden
    public static void enviar(String topic, String idWS, String mensaje) {
        productor.send(new ProducerRecord<>(topic, idWS, mensaje));
        System.out.println("[KAFKA] -> " + topic + ": " + mensaje);
    }
        // arranca el hilo que lee las ordenes (solo una vez, aunque reconectemos con el monitor)
    public static synchronized void escucharOrdenes(String idWS) {
        if (escuchando) return;
        escuchando = true;
        new ConsumidorOrdenes(broker, idWS).start();
    }
}