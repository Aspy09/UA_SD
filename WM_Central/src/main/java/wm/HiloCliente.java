package wm;

import java.io.*;
import java.net.*;

public class HiloCliente extends Thread {
    private Socket skCliente;

    public HiloCliente(Socket skCliente) {
        this.skCliente = skCliente;
    }

    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(skCliente.getInputStream()));
            String mensaje = in.readLine();
            System.out.println(mensaje);

            // Responder al cliente antes de cerrar; si no, su readLine() recibe null
            PrintWriter out = new PrintWriter(skCliente.getOutputStream(), true);
            out.println("REGISTRO_OK");

            skCliente.close();
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}