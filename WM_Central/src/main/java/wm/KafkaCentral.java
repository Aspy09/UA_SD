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
    public static final String TELEMETRIA = "wm.telemetria";
    public static final String NOTIFICACIONES = "wm.notificaciones";
    public static final String ESTACIONES = "wm.estaciones";
    // los otros modulos (engine, operario) mandan aqui sus errores: ERROR#quien#texto
    public static final String ERRORES = "wm.errores";

    // provincias: cada una tiene su propio topic de ordenes (wm.ordenes.ALICANTE...)
    // asi cada engine solo recibe las ordenes de su provincia y no las de todas
    public static final String[] REGIONES = { "ALICANTE", "VALENCIA", "MADRID", "BARCELONA", "SEVILLA" };

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
            List<String> topics = new ArrayList<>(List.of(SOLICITUDES, TELEMETRIA, NOTIFICACIONES, ESTACIONES, ERRORES));
            for (String r : REGIONES) topics.add(ordenes(r)); // y uno de ordenes por provincia
            for (String topic : topics) {
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

    // topic de ordenes de una provincia
    public static String ordenes(String region) {
        return "wm.ordenes." + region;
    }

    // si es una de nuestras provincias
    public static boolean esRegion(String region) {
        return List.of(REGIONES).contains(region);
    }

    public static String getBroker() {
        return broker;
    }
}
