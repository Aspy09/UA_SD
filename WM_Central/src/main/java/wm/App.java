package wm;

import java.io.*;
import java.net.*;

public class App {
    public static void main(String[] args) {
        
        if (args.length == 0) {
            System.out.println("Error: no se ha pasado ningun puerto");
            return;
        }

        int puerto = Integer.parseInt(args[0]);

        try (ServerSocket skServer = new ServerSocket(puerto)) {
            System.out.println("Central iniciada en el puerto " + puerto + ". Esperando...");
            
            Socket socketMonitor = skServer.accept(); 
            
            // 3. CRUCIAL: Leer lo que manda tu Monitor
            BufferedReader entrada = new BufferedReader(new InputStreamReader(socketMonitor.getInputStream()));
            System.out.println("Recibí del Monitor: " + entrada.readLine());
            
            // Le contestamos al Monitor para que no reciba null
            PrintWriter salida = new PrintWriter(socketMonitor.getOutputStream(), true);
            salida.println("REGISTRO_OK");
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}