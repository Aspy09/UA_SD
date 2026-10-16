package wm;

import java.io.*;
import java.net.*;

public class App 
{
    public static void main( String[] args ) throws  IllegalAccessException
    {
        //1.Comprueba que me han pasado el puerto y kafka por argumento
        // (args[0] y args[1]) y convierte el puerto a int

        if (args.length < 2) throw new  IllegalAccessException("uso: java -jar WM_Central.jar <puerto> <ip_kafka:puerto>");

        int puerto =Integer.parseInt(args[0]);
        String brokerKafka = args[1];

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
            new ConsumidorKafka().start();
            System.out.println("Conectada a kafka en " + brokerKafka);
        } catch (Exception e) {
            System.out.println("Error al conectar con kafka: " + e.toString());
            return;
        }

        //4.Crear un server socket en ese puerto
        
        ServerSocket skServer = null;

        try {
            skServer = new ServerSocket(puerto);
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
            return; // sin ServerSocket no podemos aceptar conexiones
        }

        System.out.println("Central escuchando en el puerto " + puerto);

        //5. esperar una conexion de un cliente
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
