package wm;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

/**
 * Envio y recepcion de tramas <STX><DATA><ETX><LRC> sobre un socket (ver README).
 * Se crea un objeto Protocolo por cada conexion.
 */
public class Protocolo {
    public static final byte STX = 0x02;
    public static final byte ETX = 0x03;
    public static final byte EOT = 0x04;
    public static final byte ENQ = 0x05;
    public static final byte ACK = 0x06;
    public static final byte NACK = 0x15;

    private static final int MAX_REINTENTOS = 3;

    private final InputStream in;
    private final OutputStream out;

    public Protocolo(Socket socket) throws IOException {
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = socket.getOutputStream();
    }

    // LRC = XOR de todos los bytes de DATA
    public static byte calcularLRC(byte[] datos) {
        byte lrc = 0;
        for (byte b : datos) {
            lrc ^= b;
        }
        return lrc;
    }

    // "OK#WS-01" -> [STX, 'O', 'K', '#', ..., ETX, LRC]
    public static byte[] empaquetar(String data) {
        byte[] datos = data.getBytes(StandardCharsets.UTF_8);
        byte[] trama = new byte[datos.length + 3];
        trama[0] = STX;
        System.arraycopy(datos, 0, trama, 1, datos.length);
        trama[datos.length + 1] = ETX;
        trama[datos.length + 2] = calcularLRC(datos);
        return trama;
    }

    // Cliente: al conectar envia ENQ y espera ACK
    public boolean enviarENQ() throws IOException {
        enviarByte(ENQ);
        return in.read() == ACK;
    }

    // Servidor: al aceptar espera ENQ y responde ACK
    public boolean esperarENQ() throws IOException {
        if (in.read() != ENQ) {
            enviarByte(NACK);
            return false;
        }
        enviarByte(ACK);
        return true;
    }

    // Envia la trama y espera ACK; si llega NACK la reenvia (hasta MAX_REINTENTOS)
    public boolean enviarTrama(String data) throws IOException {
        byte[] trama = empaquetar(data);
        for (int i = 0; i < MAX_REINTENTOS; i++) {
            out.write(trama);
            out.flush();
            int respuesta = in.read();
            if (respuesta == ACK) return true;
            if (respuesta == -1) return false; // el otro extremo cerro la conexion
        }
        return false;
    }

    // Devuelve DATA, o null si el otro extremo cierra la conexion o envia EOT.
    // Responde ACK si el LRC es correcto y NACK si no (y espera el reenvio).
    public String recibirTrama() throws IOException {
        while (true) {
            int b = in.read();
            if (b == -1 || b == EOT) return null;
            if (b != STX) continue; // cualquier byte antes del STX se ignora

            ByteArrayOutputStream datos = new ByteArrayOutputStream();
            while ((b = in.read()) != ETX) {
                if (b == -1) return null;
                datos.write(b);
            }

            int lrc = in.read();
            if (lrc == -1) return null;

            byte[] bytes = datos.toByteArray();
            if ((byte) lrc == calcularLRC(bytes)) {
                enviarByte(ACK);
                return new String(bytes, StandardCharsets.UTF_8);
            }
            enviarByte(NACK);
        }
    }

    // Aviso de cierre ordenado
    public void enviarEOT() throws IOException {
        enviarByte(EOT);
    }

    private void enviarByte(byte b) throws IOException {
        out.write(b);
        out.flush();
    }
}
