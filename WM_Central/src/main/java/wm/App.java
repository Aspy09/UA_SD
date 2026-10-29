package wm;

import java.io.*;
import java.net.*;

public class App 
{
    public static void main( String[] args ) throws  IllegalAccessException
    {
        //1.Comprueba que me han pasado el puerto y kafka por argumento
        // (args[0] y args[1]) y convierte el puerto a int

        if (args.length < 2) throw new  IllegalAccessException("uso: java -jar WM_Central.jar <puerto> <ip_kafka:puerto> [puerto_web]");

        int puerto =Integer.parseInt(args[0]);
        String brokerKafka = args[1]; //el servidor de kafka

        // puerto del panel web: 3er argumento, si no la variable PORT (la pone railway), si no 8080
        int puertoWeb = 8080;
        if (args.length >= 3) puertoWeb = Integer.parseInt(args[2]);
        else if (System.getenv("PORT") != null) puertoWeb = Integer.parseInt(System.getenv("PORT"));

        // que kafka solo muestre sus errores, no todos sus logs
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        //2.abrir la bd y cargar las estaciones (salen desconectadas)
        try {
            BaseDatos.iniciar();
            Estaciones.cargarDesdeBD();
        } catch (Exception e) {
            System.out.println("Error al abrir la base de datos: " + e.toString());
            return;
        }

        //3.conectar con kafka y empezar a leer solicitudes y telemetria en otro hilo
        try {
            KafkaCentral.iniciar(brokerKafka);
            Estaciones.publicarTodas(); // para que los fo vean las estaciones desde el principio
            new ConsumidorKafka().start();

            // cada 5 s mirar si hay riegos reservados que la estacion no ha confirmado (si su engine
            // no contesta la estacion se quedaria reservada para siempre)
            Thread vigilante = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(5000);
                        Riegos.revisarReservas();
                    } catch (Exception e) {
                        // si falla una vuelta se sigue con la siguiente
                    }
                }
            });
            vigilante.setDaemon(true); // no impide que la central se cierre
            vigilante.start();
            System.out.println("Conectada a kafka en " + brokerKafka);
        } catch (Exception e) {
            System.out.println("Error al conectar con kafka: " + e.toString());
            return;
        }

        //4.arrancar el panel web
        try {
            PanelWeb.iniciar(puertoWeb);
            System.out.println("Panel web en http://localhost:" + puertoWeb);
        } catch (Exception e) {
            System.out.println("Error al arrancar el panel web: " + e.toString());
            return;
        }

        //5.Crear un server socket en ese puerto
        
        ServerSocket skServer = null;

        try {
            skServer = new ServerSocket(puerto);
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
            return; // sin ServerSocket no podemos aceptar conexiones
        }

        System.out.println("Central escuchando en el puerto " + puerto);
        new MenuCentral().start(); // menu en la terminal para regar, bloquear, activar o dar de baja

        //6. esperar una conexion de un cliente
        for(;;){
            Socket skCliente = null;
            try {
                skCliente = skServer.accept();
                new HiloCliente(skCliente).start();
                
            } catch (Exception e) {
               System.out.println("Error: " + e.toString());
            }
        }
    }
}
