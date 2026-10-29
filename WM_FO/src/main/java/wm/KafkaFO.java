package wm;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaFO {
    // nombres de los topics 
    public static final String SOLICITUDES = "wm.solicitudes";
    public static final String NOTIFICACIONES = "wm.notificaciones";
    public static final String ESTACIONES = "wm.estaciones";
    public static final String ERRORES = "wm.errores"; // los errores se los mando a la central

    private static String broker;
    private static KafkaProducer<String, String> productor;

    public static void iniciar(String brokerKafka) {
        broker = brokerKafka;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000); // no bloquearse si kafka esta caido
        productor = new KafkaProducer<>(props);
    }

    // mandar un error a la central para que lo vea en sus alertas: ERROR#quien#texto
    public static void error(String idFO, String texto) {
        String mensaje = "ERROR#" + idFO + "#" + texto;
        productor.send(new ProducerRecord<>(ERRORES, idFO, mensaje));
        System.out.println("[KAFKA] -> " + ERRORES + ": " + mensaje);
    }

    // RIEGO#origen#id_ws#segundos; key = id de la estacion
    public static void solicitarRiego(String idFO, String idWS, int segundos) {
        String mensaje = "RIEGO#" + idFO + "#" + idWS + "#" + segundos;
        productor.send(new ProducerRecord<>(SOLICITUDES, idWS, mensaje));
        System.out.println("[KAFKA] -> " + SOLICITUDES + ": " + mensaje);
    }
}