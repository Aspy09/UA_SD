package wm;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

// acceso a la bd sqlite de la central (wm.db)
// solo guardamos lo que tiene que seguir despues de reiniciar: estaciones y operarios
// el estado en tiempo real no se guarda
public class BaseDatos {
    private static final String URL = "jdbc:sqlite:wm.db";

    // una sola conexion para toda la central, por eso los metodos son synchronized
    private static Connection conexion;

    //1.abrir la conexion y crear las tablas si no existen
    public static synchronized void iniciar() throws SQLException {
        conexion = DriverManager.getConnection(URL);

        try (Statement st = conexion.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS estaciones ("
                    + "id TEXT PRIMARY KEY, "
                    + "ubicacion TEXT NOT NULL, "
                    + "bloqueada INTEGER NOT NULL DEFAULT 0, " // sqlite no tiene boolean: 0 no, 1 si
                    + "region TEXT NOT NULL DEFAULT 'ALICANTE')");
            // si la bd es de antes y no tiene la columna region, se la añado (si ya la tiene da error y no pasa nada)
            try {
                st.execute("ALTER TABLE estaciones ADD COLUMN region TEXT NOT NULL DEFAULT 'ALICANTE'");
            } catch (SQLException e) {
                // ya existia
            }
            st.execute("CREATE TABLE IF NOT EXISTS operarios ("
                    + "id TEXT PRIMARY KEY, "
                    + "nombre TEXT NOT NULL, "
                    + "region TEXT NOT NULL DEFAULT 'ALICANTE')");
            // igual que con las estaciones: si la bd es antigua le añado la columna region
            try {
                st.execute("ALTER TABLE operarios ADD COLUMN region TEXT NOT NULL DEFAULT 'ALICANTE'");
            } catch (SQLException e) {
                // ya existia
            }
        }
        cargarOperarios();
    }

    // dar de alta los operarios de operarios.txt (va dentro del jar, una linea por operario: FO-01#nombre#provincia)
    // si ya estaban se actualizan, asi si añado operarios al fichero se dan de alta al arrancar
    private static void cargarOperarios() throws SQLException {
        String sql = "INSERT INTO operarios (id, nombre, region) VALUES (?, ?, ?) "
                + "ON CONFLICT(id) DO UPDATE SET nombre = excluded.nombre, region = excluded.region";
        try (InputStream in = BaseDatos.class.getResourceAsStream("/operarios.txt");
             PreparedStatement ps = conexion.prepareStatement(sql)) {
            if (in == null) return; // no hay fichero, no se da de alta ninguno
            BufferedReader lector = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String linea;
            while ((linea = lector.readLine()) != null) {
                String[] c = linea.trim().split("#");
                if (c.length != 3) continue; // linea vacia o mal escrita
                // con ? y setString evitamos inyeccion sql
                ps.setString(1, c[0]);
                ps.setString(2, c[1]);
                ps.setString(3, c[2]);
                ps.executeUpdate();
            }
        } catch (java.io.IOException e) {
            System.out.println("[BD] No se puede leer operarios.txt: " + e.getMessage());
        }
    }

    // todos los operarios (id, nombre, provincia), para el panel
    public static synchronized List<String[]> listarOperarios() throws SQLException {
        List<String[]> lista = new ArrayList<>();
        try (Statement st = conexion.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, nombre, region FROM operarios ORDER BY id")) {
            while (rs.next()) {
                lista.add(new String[] { rs.getString("id"), rs.getString("nombre"), rs.getString("region") });
            }
        }
        return lista;
    }

    // devuelve todas las estaciones guardadas (empiezan desconectadas)
    public static synchronized List<Estacion> cargarEstaciones() throws SQLException {
        List<Estacion> lista = new ArrayList<>();
        try (Statement st = conexion.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, ubicacion, bloqueada, region FROM estaciones")) {
            while (rs.next()) {
                Estacion ws = new Estacion(rs.getString("id"), rs.getString("ubicacion"));
                ws.setBloqueada(rs.getInt("bloqueada") == 1);
                ws.setRegion(rs.getString("region"));
                lista.add(ws);
            }
        }
        return lista;
    }

    // inserta la estacion y si ya existe actualiza sus datos
    public static synchronized void guardarEstacion(Estacion ws) throws SQLException {
        String sql = "INSERT INTO estaciones (id, ubicacion, bloqueada, region) VALUES (?, ?, ?, ?) "
                + "ON CONFLICT(id) DO UPDATE SET ubicacion = excluded.ubicacion, bloqueada = excluded.bloqueada, region = excluded.region";
        try (PreparedStatement ps = conexion.prepareStatement(sql)) {
            ps.setString(1, ws.getId());
            ps.setString(2, ws.getUbicacion());
            ps.setInt(3, ws.isBloqueada() ? 1 : 0);
            ps.setString(4, ws.getRegion());
            ps.executeUpdate();
        }
    }

    // dar de baja una estacion: borrarla de la bd
    public static synchronized void borrarEstacion(String id) throws SQLException {
        try (PreparedStatement ps = conexion.prepareStatement("DELETE FROM estaciones WHERE id = ?")) {
            ps.setString(1, id);
            ps.executeUpdate();
        }
    }

    // para comprobar las peticiones de riego de los operarios
    public static synchronized boolean existeOperario(String id) throws SQLException {
        try (PreparedStatement ps = conexion.prepareStatement("SELECT 1 FROM operarios WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }
}
