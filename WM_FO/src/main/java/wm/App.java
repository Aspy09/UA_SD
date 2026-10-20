package wm;

import java.util.Scanner;

public class App {
    private static String idFO;

    public static void main(String[] args) {
        // 1) Argumentos
        if (args.length < 2) {
            System.out.println("Uso: java -jar WM_FO.jar <ip_broker:puerto> <id_operario>");
            System.exit(1);
        }
        idFO = args[1];

        // 2) Kafka solo va a mostrar sus errores
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        KafkaFO.iniciar(args[0]);
        System.out.println("Operario " + idFO + " usando Kafka en " + args[0]);

        // 3) Menu
        menu();
    }

    private static void menu() {
        Scanner sc = new Scanner(System.in);
        mostrarMenu();
        while (sc.hasNextLine()) {
            switch (sc.nextLine().trim()) {
                case "1":
                    pedirRiegoManual(sc);
                    break;
                case "":
                    break; // volver a mostrar menu
                default:
                    System.out.println("Opcion no valida");
            }
            mostrarMenu();
        }
    }

    static void mostrarMenu() {
        System.out.println();
        System.out.println("=== Operario " + idFO + " ===");
        System.out.println("1) Pedir riego");
        System.out.print("Opcion: ");
    }

    private static void pedirRiegoManual(Scanner sc) {
        System.out.print("Estacion (ej. WS-01): ");
        if (!sc.hasNextLine()) return;
        String idWS = sc.nextLine().trim().toUpperCase();

        System.out.print("Segundos de riego: ");
        if (!sc.hasNextLine()) return;
        String segundos = sc.nextLine().trim();

        pedirRiego(idWS, segundos);
    }

    // valida y envia. Lo van a usar tanto el menu como el modo fichero 
    static boolean pedirRiego(String idWS, String textoSegundos) {
        if (idWS.isEmpty() || idWS.contains("#")) {
            System.out.println("Estacion no valida: '" + idWS + "'");
            return false;
        }
        try {
            int segundos = Integer.parseInt(textoSegundos);
            if (segundos <= 0) throw new NumberFormatException();
            KafkaFO.solicitarRiego(idFO, idWS, segundos);
            return true;
        } catch (NumberFormatException e) {
            System.out.println("Segundos no validos: '" + textoSegundos + "'");
            return false;
        }
    }
}