package wm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    // el riego en marcha se apunta en un fichero cada segundo: si el engine se cae a mitad,
    // al volver sabe lo que ya habia regado (en docker la carpeta datos es un volumen, no se pierde)
    private static Path fichero(String idWS) {
        return Path.of("datos", "riego_" + idWS + ".txt");
    }

    private void guardar(double volumen, int t) {
        try {
            Files.createDirectories(Path.of("datos"));
            Files.writeString(fichero(idWS), String.format(Locale.US, "%s#%.2f#%d", origen, volumen, t));
        } catch (IOException e) {
            // si no se puede guardar el riego sigue igual, solo se perderia si el engine se cae
        }
    }

    // al arrancar: si quedo un riego a medias por una caida, mandar su FIN con lo que se rego
    public static void recuperar(String idWS) {
        try {
            if (!Files.exists(fichero(idWS))) return;
            String[] c = Files.readString(fichero(idWS)).trim().split("#"); // origen#volumen#segundos
            KafkaEngine.enviar(KafkaEngine.TELEMETRIA, idWS, "FIN#" + idWS + "#" + c[1] + "#" + c[2] + "#CAIDA_ENGINE");
            System.out.println(">> Riego que quedo a medias por una caida (" + c[0] + "): " + c[1] + " L en " + c[2]
                    + " s, se lo mando a la central");
            Files.delete(fichero(idWS));
        } catch (Exception e) {
            System.out.println(">> No se ha podido recuperar el riego anterior: " + e.getMessage());
        }
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
        guardar(volumen, t);
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
            guardar(volumen, t);
        }

        //3.cerrar la electrovalvula y mandar el resumen
        Thread.interrupted(); // limpiar la marca de interrupcion antes de usar kafka
        String motivo = (motivoFin == null) ? "TIEMPO" : motivoFin;
        KafkaEngine.enviar(KafkaEngine.TELEMETRIA, idWS,
                String.format(Locale.US, "FIN#%s#%.2f#%d#%s", idWS, volumen, t, motivo));
        System.out.println("== Riego terminado: " + motivo);
        try {
            Files.deleteIfExists(fichero(idWS)); // ha terminado bien, ya no hay nada que recuperar
        } catch (IOException e) {
            // nada
        }
        App.mostrarMenu(); // volver a mostrar menu
        Estacion.riegoTerminado(this);
    }
}