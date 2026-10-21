package wm;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, idWS); // cada engine su grupo: todos reciben todas las ordenes
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // si el engine muere sin despedirse, kafka lo expulsa a los 10 s (por defecto 45)
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
            //2.suscribirse a las ordenes; al arrancar saltar al final para ignorar las antiguas
            consumidor.subscribe(List.of(KafkaEngine.ORDENES), new ConsumerRebalanceListener() {
                public void onPartitionsAssigned(Collection<TopicPartition> particiones) {
                    consumidor.seekToEnd(particiones);
                    for (TopicPartition p : particiones) {
                        consumidor.position(p); // calcular la posicion ya (seekToEnd es perezoso)
                    }
                    // solo estamos listos si kafka nos ha dado particiones de verdad
                    if (!particiones.isEmpty()) {
                        System.out.println("[KAFKA] Listo para recibir ordenes de " + idWS);
                        App.iniciarMenu();
                    }
                }
                public void onPartitionsRevoked(Collection<TopicPartition> particiones) {
                }
            });

            // al cerrar el programa (Ctrl+C o docker stop) despedirse del grupo
            Thread hiloConsumidor = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumidor.wakeup(); // hace que poll() lance WakeupException
                try {
                    hiloConsumidor.join(3000); // esperar a que se cierre bien
                } catch (InterruptedException e) {
                    // nada
                }
            }));

            //3.pedir mensajes nuevos todo el rato (si no hay, espera 1 s)
            while (true) {
                for (ConsumerRecord<String, String> m : consumidor.poll(Duration.ofSeconds(1))) {
                    String[] c = m.value().split("#");
                    // ORDEN#id_ws#... -> solo las nuestras o las de TODAS
                    if (c.length < 2) continue;
                    if (!c[1].equals(idWS) && !c[1].equals("TODAS")) continue;

                    System.out.println("[KAFKA] <- orden: " + m.value());
                    // try dentro del for: una orden mal escrita no debe parar el hilo
                    try {
                        Estacion.procesarOrden(c);
                    } catch (Exception e) {
                        System.out.println("[KAFKA] Orden incorrecta (" + m.value() + "): " + e);
                    }
                }
            }
        } catch (WakeupException e) {
            //  cierre normal. try-with-resources hace close() y kafka nos saca del grupo
        } catch (Exception e) {
            System.out.println("[KAFKA] Error en el consumidor de ordenes: " + e);
        }
    }
}