package wm;

import java.io.*;
import java.net.*;

public class ClientePrueba {
    public static void main(String[] args) throws IOException {
        Socket skCliente = new Socket("localhost", 9999);
        PrintWriter out = new PrintWriter(skCliente.getOutputStream(), true);
        out.println("REGISTRO#WS-01");
        skCliente.close();
    }
}