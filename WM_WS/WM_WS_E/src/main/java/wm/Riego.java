package wm;

import java.util.Locale;
import java.util.Random;

// un riego en marcha: un hilo que cada segundo envia el caudal y el volumen
public class Riego extends Thread {
    private final String idWS;
    private final String origen;
    private final int segundos;
    private volatile String motivoFin = null; // null = sigue regando

    public Riego(String idWS, String origen, int segundos) {
        this.idWS = idWS;
        this.origen = origen;
        this.segundos = segundos;
    }

    // lo llaman otros hilos para cortar el riego
    public void parar(String motivo) {
        if (motivoFin == null) motivoFin = motivo;
        interrupt(); // despierta al hilo si esta dormido en el sleep
    }

    public void run() {
        //1.abrir la electrovalvula
        KafkaEngine.enviar(KafkaEngine.TELEMETRIA, idWS, "INICIADO#" + idWS);
        System.out.println("== Riego iniciado (" + origen + ", " + segundos + " s)");

        //2.cada segundo: caudal aleatorio y volumen acumulado
        Random rnd = new Random();
        double volumen = 0;
        int t = 0;
        while (motivoFin == null && t < segundos) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break; // nos pararon
            }
            t++;
            double caudal = 10 + rnd.nextDouble() * 4; // entre 10 y 14 L/min
            volumen += caudal / 60;                    // litros de este segundo
            KafkaEngine.enviar(KafkaEngine.TELEMETRIA, idWS,
                    String.format(Locale.US, "RIEGO#%s#%.1f#%.2f", idWS, caudal, volumen));
        }

        //3.cerrar la electrovalvula y mandar el resumen
        Thread.interrupted(); // limpiar la marca de interrupcion antes de usar kafka
        String motivo = (motivoFin == null) ? "TIEMPO" : motivoFin;
        KafkaEngine.enviar(KafkaEngine.TELEMETRIA, idWS,
                String.format(Locale.US, "FIN#%s#%.2f#%d#%s", idWS, volumen, t, motivo));
        System.out.println("== Riego terminado: " + motivo);
        Estacion.riegoTerminado(this);
    }
}