package wm;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

// hilo que lee las ordenes de central y se queda solo con las de esta estacion
public class ConsumidorOrdenes extends Thread {
    private final String broker;
    private final String idWS;

    public ConsumidorOrdenes(String broker, String idWS) {
        this.broker = broker;
        this.idWS = idWS;
    }

    public void run() {
        //1.configurar el consumidor
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, idWS); // cada engine su grupo. Todos reciben todas las ordenes
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            //2.suscribirse a las ordenes; al arrancar saltar al final para ignorar las antiguas
            consumidor.subscribe(List.of(KafkaEngine.ORDENES), new ConsumerRebalanceListener() {
                public void onPartitionsAssigned(Collection<TopicPartition> particiones) {
                    consumidor.seekToEnd(particiones);
                    System.out.println("[KAFKA] Listo para recibir ordenes de " + idWS);
                    App.iniciarMenu();

                }
                public void onPartitionsRevoked(Collection<TopicPartition> particiones) {
                }
            });

            //3. Pedir mensajes nuevos todo el rato (si no hay espera1 s)
            while (true) {
                ConsumerRecords<String, String> mensajes = consumidor.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> m : mensajes) {
                    String[] c = m.value().split("#");
                    // ORDEN#id_ws#... -> solo las nuestras o las de TODAS
                    if (c.length < 2) continue;
                    if (!c[1].equals(idWS) && !c[1].equals("TODAS")) continue;

                    System.out.println("[KAFKA] <- orden: " + m.value());

                    // una orden mal escrita no debe parar el hilo
                    try {
                        Estacion.procesarOrden(c);
                    } catch (Exception e) {
                        System.out.println("[KAFKA] Orden incorrecta (" + m.value() + "): " + e);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[KAFKA] Error en el consumidor de ordenes: " + e);
        }
    }
}