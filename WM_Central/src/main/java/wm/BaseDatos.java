package wm;

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
                    + "nombre TEXT NOT NULL)");
        }
        crearOperariosDePrueba();
    }

    // si no hay operarios metemos unos cuantos para probar
    private static void crearOperariosDePrueba() throws SQLException {
        try (Statement st = conexion.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM operarios")) {
            if (rs.getInt(1) > 0) return;
        }

        String[][] operarios = { { "FO-01", "Ana" }, { "FO-02", "Luis" }, { "FO-03", "Marta" } };

        // con ? y setString evitamos inyeccion sql
        try (PreparedStatement ps = conexion.prepareStatement("INSERT INTO operarios (id, nombre) VALUES (?, ?)")) {
            for (String[] op : operarios) {
                ps.setString(1, op[0]);
                ps.setString(2, op[1]);
                ps.executeUpdate();
            }
        }
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
