package wm;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

// hilo que lee de kafka las solicitudes de los fo y la telemetria de los engines
public class ConsumidorKafka extends Thread {

    public void run() {
        //1.configurar el consumidor
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaCentral.getBroker());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // como --from-beginning
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            //2.escuchar los dos topics
            consumidor.subscribe(List.of(KafkaCentral.SOLICITUDES, KafkaCentral.TELEMETRIA));

            //3.pedir mensajes nuevos todo el rato (si no hay espera 1s)
            while (true) {
                ConsumerRecords<String, String> mensajes = consumidor.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> m : mensajes) {
                    // TODO: procesar segun el topic
                    System.out.println("[KAFKA] <- " + m.topic() + ": " + m.value());
                }
            }
        } catch (Exception e) {
            System.out.println("[KAFKA] Error en el consumidor: " + e.toString());
        }
    }
}
