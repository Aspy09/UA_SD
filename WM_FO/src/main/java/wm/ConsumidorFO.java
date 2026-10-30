package wm;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, idFO); // cada operario su grupo (readme)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // si el operario muere sin despedirse, kafka lo expulsa a los 10 s (por defecto 45)
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            //2.dos topics, cada uno leido desde un sitio distinto
            consumidor.subscribe(List.of(KafkaFO.NOTIFICACIONES, KafkaFO.ESTACIONES),
                    new ConsumerRebalanceListener() {
                public void onPartitionsAssigned(Collection<TopicPartition> particiones) {
                    for (TopicPartition p : particiones) {
                        if (p.topic().equals(KafkaFO.ESTACIONES)) {
                            consumidor.seekToBeginning(List.of(p)); // todo el historial: estado de todas
                        } else if (consumidor.committed(Set.of(p)).get(p) == null) {
                            consumidor.seekToEnd(List.of(p));       // notificaciones, primera vez: solo las nuevas
                        }
                        // si no, kafka sigue desde donde lo dejo este operario: si se cerro durante
                        // un riego, al volver le llegan los avisos que se perdio (y el RESUMEN)
                    }
                    // los seekTo son "perezosos": position() calcula la posicion ya
                    for (TopicPartition p : particiones) {
                        consumidor.position(p);
                    }
                    // solo estamos listos si kafka nos dio particiones de verdad
                    if (!particiones.isEmpty()) listo.countDown();
                }
                public void onPartitionsRevoked(Collection<TopicPartition> particiones) {
                }
            });

            // al cerrar el programa (fin normal, Ctrl+C o docker stop) despedirse del grupo
            Thread hiloConsumidor = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumidor.wakeup(); // hace que poll() lance WakeupException
                try {
                    hiloConsumidor.join(3000); // esperar a que se cierre bien
                } catch (InterruptedException e) {
                    // nada
                }
            }));

            //3.leer mensajes todo el rato
            while (true) {
                for (ConsumerRecord<String, String> m : consumidor.poll(Duration.ofSeconds(1))) {
                    // try dentro del for: un mensaje mal escrito no debe parar el hilo
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
        } catch (WakeupException e) {
            // cierre normal. try-with-resources hace close() y kafka nos saca del grupo
        } catch (Exception e) {
            System.out.println("[KAFKA] Error en el consumidor: " + e);
        }
    }
}