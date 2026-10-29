import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseInitializer {

    private static final String DB_URL = "jdbc:sqlite:" +
            (System.getenv("DB_PATH") != null ? System.getenv("DB_PATH") : "riego_central.db");

    public static Connection conectar() throws SQLException {
        Connection con = DriverManager.getConnection(DB_URL);
        try (Statement st = con.createStatement()) {
            st.execute("PRAGMA busy_timeout = 5000;");
        }
        return con;
    }

    public static void crearBaseDeDatos() {
        // Sentencias SQL para las tablas requeridas según especificación
        String sqlEstaciones = "CREATE TABLE IF NOT EXISTS estaciones ("
                + "id TEXT PRIMARY KEY, "
                + "ubicacion TEXT NOT NULL, "
                + "estado TEXT NOT NULL CHECK(estado IN ('DISPONIBLE', 'REGANDO', 'LEAK', 'FUERA_DE_SERVICIO', 'DESCONECTADA')), "
                + "caudal_litros_min REAL DEFAULT 0.0, "
                + "volumen_acumulado_litros REAL DEFAULT 0.0, "
                + "id_operario_actual TEXT, "
                + "ultima_conexion TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
                + ");";

        String sqlOperarios = "CREATE TABLE IF NOT EXISTS operarios ("
                + "id TEXT PRIMARY KEY, "
                + "nombre TEXT NOT NULL, "
                + "activo INTEGER DEFAULT 1"
                + ");";

        String sqlHistoricoRiegos = "CREATE TABLE IF NOT EXISTS historico_riegos ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                + "id_estacion TEXT NOT NULL, "
                + "id_operario TEXT NOT NULL, "
                + "fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "fecha_fin TIMESTAMP, "
                + "volumen_total_litros REAL DEFAULT 0.0, "
                + "duracion_segundos INTEGER DEFAULT 0, "
                + "estado_final TEXT CHECK(estado_final IN ('COMPLETADO', 'INTERRUMPIDO_LEAK', 'BLOQUEADO_CENTRAL')), "
                + "FOREIGN KEY (id_estacion) REFERENCES estaciones(id), "
                + "FOREIGN KEY (id_operario) REFERENCES operarios(id)"
                + ");";

        // Carga de datos iniciales para pruebas de laboratorio
        String insertOperarios = "INSERT OR IGNORE INTO operarios (id, nombre) VALUES "
                + "('FO-01', 'Juan Pérez'), "
                + "('FO-05', 'María García'), "
                + "('FO-12', 'Carlos López'), "
                + "('CENTRAL', 'Central de control'), "
                + "('MANUAL', 'Manual en la estacion');";

        String insertEstaciones = "INSERT OR IGNORE INTO estaciones (id, ubicacion, estado) VALUES "
                + "('WS-01', 'River Park', 'DESCONECTADA'), "
                + "('WS-02', 'Riverside Gardens', 'DESCONECTADA'), "
                + "('WS-03', 'Central Park', 'DESCONECTADA'), "
                + "('WS-04', 'North Roundabout', 'DESCONECTADA');";

        Connection con = null;
        try {
            con = conectar();
            Statement stmt = con.createStatement();

            // Habilitar Foreign Keys en SQLite
            stmt.execute("PRAGMA foreign_keys = ON;");

            stmt.execute(sqlEstaciones);
            stmt.execute(sqlOperarios);
            stmt.execute(sqlHistoricoRiegos);

            stmt.execute(insertOperarios);
            stmt.execute(insertEstaciones);

            System.out.println(" Base de datos 'riego_central.db' creada e inicializada correctamente.");

        } catch (SQLException ex) {
            System.err.println("Error al crear la BD: " + ex.getMessage());
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    System.err.println("Error al cerrar conexion: " + ex.getMessage());
                }
            }
        }
    }
}