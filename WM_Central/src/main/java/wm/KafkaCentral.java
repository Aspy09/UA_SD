package wm;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// la central con kafka: crear los topics y enviar mensajes
public class KafkaCentral {
    // nombres de los topics (los del readme)
    public static final String SOLICITUDES = "wm.solicitudes";
    public static final String ORDENES = "wm.ordenes";
    public static final String TELEMETRIA = "wm.telemetria";
    public static final String NOTIFICACIONES = "wm.notificaciones";
    public static final String ESTACIONES = "wm.estaciones";

    private static String broker;
    private static KafkaProducer<String, String> productor;

    public static void iniciar(String brokerKafka) throws Exception {
        broker = brokerKafka;

        //1.crear los topics que falten
        crearTopics();

        //2.crear el productor, los mensajes son texto
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        productor = new KafkaProducer<>(props);
    }

    private static void crearTopics() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        // si kafka esta apagado que de error a los 10s
        props.put("request.timeout.ms", "5000");
        props.put("default.api.timeout.ms", "10000");

        try (AdminClient admin = AdminClient.create(props)) {
            //1.ver que topics hay ya
            Set<String> existentes = admin.listTopics().names().get();

            //2.apuntar los que no estan
            List<NewTopic> nuevos = new ArrayList<>();
            for (String topic : new String[] { SOLICITUDES, ORDENES, TELEMETRIA, NOTIFICACIONES, ESTACIONES }) {
                if (!existentes.contains(topic)) {
                    nuevos.add(new NewTopic(topic, 1, (short) 1)); // 1 particion y 1 replica
                }
            }

            //3.crearlos
            if (!nuevos.isEmpty()) {
                admin.createTopics(nuevos).all().get();
                System.out.println("[KAFKA] Topics creados: " + nuevos.size());
            }
        }
    }

    // key = id de la ws o del fo, asi sus mensajes llegan en orden
    public static void enviar(String topic, String key, String mensaje) {
        productor.send(new ProducerRecord<>(topic, key, mensaje));
        System.out.println("[KAFKA] -> " + topic + ": " + mensaje);
    }

    public static String getBroker() {
        return broker;
    }
}
