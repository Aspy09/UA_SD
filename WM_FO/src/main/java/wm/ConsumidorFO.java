package wm;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

// hilo que lee las notificaciones para este operario y el estado de todas las estaciones
public class ConsumidorFO extends Thread {
    private final String broker;
    private final String idFO;
    private final CountDownLatch listo; // avisa al main cuando ya podemos recibir

    public ConsumidorFO(String broker, String idFO, CountDownLatch listo) {
        this.broker = broker;
        this.idFO = idFO;
        this.listo = listo;
        setDaemon(true); // que no impida cerrar el programa
    }

    public void run() {
        //1.configurar el consumidor
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, idFO); // cada operario su grupo 
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            //2.dos topics, cada uno leido desde un lugar distinto
            consumidor.subscribe(List.of(KafkaFO.NOTIFICACIONES, KafkaFO.ESTACIONES),
                    new ConsumerRebalanceListener() {
                public void onPartitionsAssigned(Collection<TopicPartition> particiones) {
                    for (TopicPartition p : particiones) {
                        if (p.topic().equals(KafkaFO.ESTACIONES)) {
                            consumidor.seekToBeginning(List.of(p)); // todo el historial: estado de todas
                        } else {
                            consumidor.seekToEnd(List.of(p));       // notificaciones: solo las nuevas
                        }
                    }
                    listo.countDown();
                }
                public void onPartitionsRevoked(Collection<TopicPartition> particiones) {
                }
            });

            //3.leer mensajes todo el rato
            while (true) {
                for (ConsumerRecord<String, String> m : consumidor.poll(Duration.ofSeconds(1))) {
                    //asi un mensaje mal escrito no para todo
                    try {
                        if (m.topic().equals(KafkaFO.ESTACIONES)) {
                            App.actualizarEstacion(m.value());
                        } else {
                            App.notificacion(m.value());
                        }
                    } catch (Exception e) {
                        System.out.println("[KAFKA] Mensaje incorrecto (" + m.value() + "): " + e);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[KAFKA] Error en el consumidor: " + e);
        }
    }
}