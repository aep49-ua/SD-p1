import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EstacionDAO {

    /**
     * Procesa la Alta o Registro de una estación en SQLite (UPSERT).
     * Si no existe, realiza un INSERT.
     * Si ya existe, actualiza su ubicación y cambia su estado a 'DISPONIBLE'.
     */
    public static void registrarOAltaEstacion(String idEstacion, String ubicacion) throws Exception {
        String sql = "INSERT INTO estaciones (id, ubicacion, estado, ultima_conexion) " +
                     "VALUES (?, ?, 'DISPONIBLE', CURRENT_TIMESTAMP) " +
                     "ON CONFLICT(id) DO UPDATE SET " +
                     "ubicacion = excluded.ubicacion, " +
                     "estado = 'DISPONIBLE', " +
                     "ultima_conexion = CURRENT_TIMESTAMP;";
                     
        Connection con = null;
        try {
            con = DatabaseInitializer.conectar();
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setString(1, idEstacion);
                pstmt.setString(2, ubicacion);
                pstmt.executeUpdate();
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (registrarOAltaEstacion): " + ex.getMessage());
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    System.err.println("Error al cerrar conexión: " + ex.getMessage());
                }
            }
        }
    }

    public static String obtenerEstado(String idEstacion) throws Exception {
        String sql = "SELECT estado FROM estaciones WHERE id = ?";
        String estado = "DESCONECTADA";

        Connection con = null;
        try {
            con = DatabaseInitializer.conectar();
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setString(1, idEstacion);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        estado = rs.getString("estado");
                    }
                }
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (obtenerEstado): " + ex.getMessage());
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    System.err.println("Error al cerrar conexión: " + ex.getMessage());
                }
            }
        }
        return estado;
    }

    /**
     * Actualiza el estado de una estación en la base de datos (ej. 'LEAK', 'DESCONECTADA', 'DISPONIBLE').
     */
    public static void actualizarEstado(String idEstacion, String estado) throws Exception {
        String sql = "UPDATE estaciones SET estado = ?, ultima_conexion = CURRENT_TIMESTAMP WHERE id = ?";
        
        Connection con = null;
        try {
            con = DatabaseInitializer.conectar();
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setString(1, estado);
                pstmt.setString(2, idEstacion);
                pstmt.executeUpdate();
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (actualizarEstado): " + ex.getMessage());
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException ex) {
                    System.err.println("Error al cerrar conexión: " + ex.getMessage());
                }
            }
        }
    }
}