package wm;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

// menu de la central en la terminal, hace lo mismo que los botones del panel
// (en railway no hay teclado: hasNextLine da false y el hilo se acaba sin mas)
public class MenuCentral extends Thread {

    public MenuCentral() {
        setDaemon(true); // no impide que la central se cierre
    }

    public void run() {
        Scanner sc = new Scanner(System.in);
        mostrarMenu();
        while (sc.hasNextLine()) {
            switch (sc.nextLine().trim()) {
                case "1":
                    verEstaciones();
                    break;
                case "2":
                    orden(sc, "riego");
                    break;
                case "3":
                    orden(sc, "bloquear");
                    break;
                case "4":
                    orden(sc, "activar");
                    break;
                case "5":
                    orden(sc, "baja");
                    break;
                case "":
                    break; // Enter suelto vuelve a mostrar el menu
                default:
                    System.out.println("Opcion no valida");
            }
            mostrarMenu();
        }
    }

    private static void mostrarMenu() {
        System.out.println();
        System.out.println("=== CENTRAL ===");
        System.out.println("1) Ver estaciones  2) Regar  3) Bloquear (fuera de servicio)  4) Activar  5) Dar de baja");
        System.out.print("Opcion: ");
    }

    private static void verEstaciones() {
        if (Estaciones.todas().isEmpty()) {
            System.out.println("No hay ninguna estacion registrada");
            return;
        }
        for (Estacion ws : Estaciones.todas()) {
            System.out.println(ws.getId() + " - " + ws.getUbicacion() + " (" + ws.getRegion() + "): " + ws.getEstado());
        }
    }

    // pide a que estacion (o TODAS) y manda la orden a cada una
    private static void orden(Scanner sc, String accion) {
        System.out.print("Estacion (WS-05 o TODAS): ");
        if (!sc.hasNextLine()) return;
        String idWS = sc.nextLine().trim().toUpperCase();

        //1.a que estaciones va la orden, con TODAS se puede elegir una provincia
        List<String> ids = new ArrayList<>();
        if (idWS.equals("TODAS")) {
            System.out.print("Provincia (Enter = todas las provincias): ");
            if (!sc.hasNextLine()) return;
            String region = sc.nextLine().trim().toUpperCase();
            for (Estacion ws : Estaciones.todas()) {
                if (region.isEmpty() || ws.getRegion().equals(region)) ids.add(ws.getId());
            }
        } else if (Estaciones.region(idWS) == null) {
            System.out.println(idWS + " no existe");
            return;
        } else {
            ids.add(idWS);
        }

        int segundos = 0;
        if (accion.equals("riego")) {
            System.out.print("Segundos de riego: ");
            if (!sc.hasNextLine()) return;
            try {
                segundos = Integer.parseInt(sc.nextLine().trim());
                if (segundos <= 0) throw new NumberFormatException();
            } catch (NumberFormatException e) {
                System.out.println("Hay que poner un numero de segundos mayor que 0");
                return;
            }
        }

        //2.ejecutarla en cada una (igual que en PanelWeb)
        for (String id : ids) {
            switch (accion) {
                case "riego":
                    Riegos.solicitarRiego("CENTRAL", id, segundos); // como si lo pidiera un operario
                    break;
                case "bloquear":
                    Mensajes.add("CENTRAL bloquea " + id);
                    Estaciones.bloquear(id);
                    break;
                case "activar":
                    Mensajes.add("CENTRAL activa " + id);
                    Estaciones.activar(id);
                    break;
                case "baja":
                    String motivo = Estaciones.darDeBaja(id); // solo las desconectadas
                    if (motivo != null) System.out.println("No se puede dar de baja " + id + ": " + motivo);
                    break;
            }
        }
        System.out.println("Orden enviada a " + ids.size() + " estacion(es)");
    }
}
