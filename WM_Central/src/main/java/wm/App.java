package wm;

import java.io.*;
import java.net.*;

public class App 
{
    public static void main( String[] args ) throws  IllegalAccessException
    {
        //1.Comprueba que me han pasado el puerto por argumento 
        // (args[0]) y conviertelo a int

        if (args.length == 0) throw new  IllegalAccessException("no se ha pasado ningun puerto");

        int puerto =Integer.parseInt(args[0]);

        //2.Crear un server socket en ese puerto
        
        ServerSocket skServer = null;

        try {
            skServer = new ServerSocket(puerto);
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
            return; // sin ServerSocket no podemos aceptar conexiones
        }

        System.out.println("Central escuchando en el puerto " + puerto);

        //3. esperar una conexion de un cliente 
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
