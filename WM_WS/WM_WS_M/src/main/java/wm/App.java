package wm;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;

public class App 
{
    public static void main( String[] args ){
        if (args.length < 3){
            System.out.println("Error. Uso: java App <puerto_monitor> <ip:puerto_central> <id_ws>");
            System.exit(1);
        }

        String puertoLocal = args[0];
        String centralInfo= args[1];  // Ej: 192.0.0.1:8000 IP:PUERTO 
        String idEstacion = args[2]; // Ej: WS-01
        

        // Obtener ip y puerto de la central
        // Puerto tiene que ser si o si un int
        String[] puerto_e_ip_Central = centralInfo.split(":");
        String ipCentral = puerto_e_ip_Central[0];
        int puertoCentral= Integer.parseInt(puerto_e_ip_Central[1]);


        //*******MONITOR ACTUA DE CLIENTE*******

        /*
            Argumentos del try:
         1. Crear socket y conectarnos
         2. Enviar bytes mediante sockets agrupandolos mediante printWriter
         3. Agrupa y guarda (Buffer reader) en un buffer los bytes del socket recibidos (getINput)
            y decodificados (por inputStreamReader)
         */

        try(Socket socketCentral = new Socket(ipCentral, puertoCentral);
            PrintWriter salida = new PrintWriter(socketCentral.getOutputStream(), true);
            BufferedReader entrada = new BufferedReader(new InputStreamReader(socketCentral.getInputStream()))){

            
            System.out.println("Conectado a la CENTRAL. Registrando estación...");

            //TODO: <STX><DATA><ETX><LRC> Formato sugerido por el pdf. Ejm: <STX>REGISTRO#WS-01<ETX>Z
            salida.println("REGISTRO#" + idEstacion); // por ahora el id con la etiqueta nomas para probar

            
            String respuestaCentral = entrada.readLine();
            System.out.println("La central respondio: "+ respuestaCentral);

        }catch(Exception e){
            System.out.println("Error al conectar a la central: " + e.getMessage());
        }

        //*******MONITOR ACTUA DE SERVIDOR*******

        int numero_puertoLocal = Integer.parseInt(puertoLocal);

        try(ServerSocket socketLocal = new ServerSocket(numero_puertoLocal)){
            System.out.println("Esperando conexión del Engine...");
            Socket socketEngine = socketLocal.accept();

        }catch(Exception e){
            System.out.println("Error al iniciar servidor local "+ e.getMessage());
        }

    }
}
