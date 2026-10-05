package wm;

import java.io.*;
import java.net.*;

import javax.imageio.IIOException;;

public class App 
{
    public static void main( String[] args ) throws IIOException
    {
        //1.Comprueba que me han pasado el puerto por argumento 
        // (args[0]) y conviertelo a int

        if (args[0].isEmpty()) throw new  IIOException("no se ha pasado ningun puerto");

        int puerto =Integer.parseInt(args[0]);

        //2.Crear un server socket en ese puerto

        try {
            ServerSocket skServer = new ServerSocket(puerto);
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}
