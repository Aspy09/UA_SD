package wm;

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
        String centralInfo= args[1];  // Ej: IP:PUERTO  000.0.0.1:8000
        String idEstacion = args[2]; // Ej: WS-01
        

        // Obtener ip y puerto de la central
        // Puerto tiene que ser si o si un int
        String[] puerto_e_ip_Central = centralInfo.split(":");
        String ipCentral = puerto_e_ip_Central[0];
        int puertoCentral= Integer.parseInt(puerto_e_ip_Central[1]);

        // Crear socket y conectarnos
        // Enviar info mediante sockets envolviendola mediante printWriter
        try(Socket socketCentral = new Socket(ipCentral, puertoCentral);
            PrintWriter salida = new PrintWriter(socketCentral.getOutputStream(), true)){

            System.out.println("Conectado a la CENTRAL. Registrando estación...");

            //TODO: <STX><DATA><ETX><LRC> Formato sugerido. Ejm: <STX>REGISTRO#WS-01<ETX>Z
            salida.println("REGISTRO#" + idEstacion); // por ahora el id con la etiqueta nomas para probar

        }catch(Exception e){
            System.out.println("Error al conectar a la central: " + e.getMessage());
        }

    }
}
