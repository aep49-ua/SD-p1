import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class EstacionDAO {


    // al arrancar Central todas las estaciones pasan a DESCONECTADA
    public static void marcarTodasDesconectadas() throws Exception {
        try (Connection con = DatabaseInitializer.conectar();
             java.sql.Statement st = con.createStatement()) {
            st.executeUpdate("UPDATE estaciones SET estado = 'DESCONECTADA'");
        } catch (SQLException ex) {
            throw new Exception("Error BD (marcarTodasDesconectadas): " + ex.getMessage());
        }
    }

    // un riego abierto es el que no tiene fecha_fin en el histórico
    public static boolean hayRiegoAbierto(String idEstacion) throws Exception {
        String sql = "SELECT 1 FROM historico_riegos WHERE id_estacion = ? AND fecha_fin IS NULL";
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, idEstacion);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (hayRiegoAbierto): " + ex.getMessage());
        }
    }

    // estaciones con un riego abierto cuya última señal es de hace más de los segundos indicados
    public static List<String> riegosAbandonados(int segundosSinNoticias) throws Exception {
        String sql = "SELECT DISTINCT h.id_estacion FROM historico_riegos h JOIN estaciones e ON e.id = h.id_estacion " +
                     "WHERE h.fecha_fin IS NULL AND (julianday(CURRENT_TIMESTAMP) - julianday(e.ultima_conexion)) * 86400 > ?";
        List<String> lista = new ArrayList<>();
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setInt(1, segundosSinNoticias);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    lista.add(rs.getString(1));
                }
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (riegosAbandonados): " + ex.getMessage());
        }
        return lista;
    }

    // guarda en la BD si Central ha bloqueado la estación
    public static void marcarFueraDeServicio(String idEstacion, boolean fuera) throws Exception {
        String sql = "UPDATE estaciones SET fuera_servicio = ? WHERE id = ?";
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setInt(1, fuera ? 1 : 0);
            pstmt.setString(2, idEstacion);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new Exception("Error BD (marcarFueraDeServicio): " + ex.getMessage());
        }
    }

    // comprueba si la estación está bloqueada por Central
    public static boolean estaFueraDeServicio(String idEstacion) throws Exception {
        String sql = "SELECT 1 FROM estaciones WHERE id = ? AND fuera_servicio = 1";
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, idEstacion);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (estaFueraDeServicio): " + ex.getMessage());
        }
    }

    // el operario tiene que estar en la tabla y estar activo
    public static boolean operarioValido(String idOperario) throws Exception {
        String sql = "SELECT 1 FROM operarios WHERE id = ? AND activo = 1";
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, idOperario);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (operarioValido): " + ex.getMessage());
        }
    }

    // cambia el estado solo si ahora tiene el estado requerido; devuelve si ha cambiado algo
    public static boolean actualizarEstadoSi(String idEstacion, String nuevo, String requerido) throws Exception {
        String sql = "UPDATE estaciones SET estado = ?, ultima_conexion = CURRENT_TIMESTAMP " +
                     "WHERE id = ? AND estado = ?";
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, nuevo);
            pstmt.setString(2, idEstacion);
            pstmt.setString(3, requerido);
            return pstmt.executeUpdate() > 0;
        } catch (SQLException ex) {
            throw new Exception("Error BD (actualizarEstadoSi): " + ex.getMessage());
        }
    }

    // pone la estación en REGANDO y abre una fila nueva en el histórico, sin fecha_fin hasta que acabe
    public static void iniciarRiego(String idEstacion, String idOperario) throws Exception {
        try (Connection con = DatabaseInitializer.conectar()) {
            try (PreparedStatement p = con.prepareStatement(
                    "UPDATE estaciones SET estado = 'REGANDO', id_operario_actual = ?, caudal_litros_min = 0, " +
                    "volumen_acumulado_litros = 0, ultima_conexion = CURRENT_TIMESTAMP WHERE id = ?")) {
                p.setString(1, idOperario);
                p.setString(2, idEstacion);
                p.executeUpdate();
            }
            try (PreparedStatement p = con.prepareStatement(
                    "INSERT INTO historico_riegos (id_estacion, id_operario) VALUES (?, ?)")) {
                p.setString(1, idEstacion);
                p.setString(2, idOperario);
                p.executeUpdate();
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (iniciarRiego): " + ex.getMessage());
        }
    }

    // guarda el último caudal y volumen que manda la estación
    public static void actualizarTelemetria(String idEstacion, double caudal, double volumen) throws Exception {
        String sql = "UPDATE estaciones SET caudal_litros_min = ?, volumen_acumulado_litros = ?, " +
                     "ultima_conexion = CURRENT_TIMESTAMP WHERE id = ?";
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setDouble(1, caudal);
            pstmt.setDouble(2, volumen);
            pstmt.setString(3, idEstacion);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new Exception("Error BD (actualizarTelemetria): " + ex.getMessage());
        }
    }

    // volumen acumulado hasta ahora en el riego en curso
    public static double obtenerVolumen(String idEstacion) throws Exception {
        String sql = "SELECT volumen_acumulado_litros FROM estaciones WHERE id = ?";
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, idEstacion);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getDouble(1) : 0.0;
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (obtenerVolumen): " + ex.getMessage());
        }
    }

    // cierra el riego abierto con su estado final y devuelve el operario para avisarle; con duración negativa la calcula a partir de la fecha de inicio
    public static String finalizarRiego(String idEstacion, String estadoFinal, double volumen, int duracion) throws Exception {
        try (Connection con = DatabaseInitializer.conectar()) {
            String operario = null;
            int idHistorico = -1;
            try (PreparedStatement p = con.prepareStatement(
                    "SELECT id, id_operario FROM historico_riegos WHERE id_estacion = ? AND fecha_fin IS NULL " +
                    "ORDER BY id DESC LIMIT 1")) {
                p.setString(1, idEstacion);
                try (ResultSet rs = p.executeQuery()) {
                    if (rs.next()) {
                        idHistorico = rs.getInt(1);
                        operario = rs.getString(2);
                    }
                }
            }
            try (PreparedStatement p = con.prepareStatement(
                    "UPDATE estaciones SET caudal_litros_min = 0, volumen_acumulado_litros = 0, " +
                    "id_operario_actual = NULL WHERE id = ?")) {
                p.setString(1, idEstacion);
                p.executeUpdate();
            }
            if (idHistorico < 0) {
                return null;
            }
            try (PreparedStatement p = con.prepareStatement(
                    "UPDATE historico_riegos SET fecha_fin = CURRENT_TIMESTAMP, volumen_total_litros = ?, " +
                    "duracion_segundos = CASE WHEN ? < 0 THEN CAST((julianday(CURRENT_TIMESTAMP) - " +
                    "julianday(fecha_inicio)) * 86400 AS INTEGER) ELSE ? END, estado_final = ? WHERE id = ?")) {
                p.setDouble(1, volumen);
                p.setInt(2, duracion);
                p.setInt(3, duracion);
                p.setString(4, estadoFinal);
                p.setInt(5, idHistorico);
                p.executeUpdate();
            }
            return operario;
        } catch (SQLException ex) {
            throw new Exception("Error BD (finalizarRiego): " + ex.getMessage());
        }
    }

    // devuelve todas las estaciones como filas de texto para el panel y para publicarlas
    public static List<String[]> listarEstaciones() throws Exception {
        String sql = "SELECT id, ubicacion, estado, caudal_litros_min, volumen_acumulado_litros, " +
                     "id_operario_actual, ultima_conexion FROM estaciones ORDER BY id";
        List<String[]> lista = new ArrayList<>();
        try (Connection con = DatabaseInitializer.conectar();
             PreparedStatement pstmt = con.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                lista.add(new String[] {
                    rs.getString(1), rs.getString(2), rs.getString(3),
                    String.format(java.util.Locale.US, "%.1f", rs.getDouble(4)),
                    String.format(java.util.Locale.US, "%.1f", rs.getDouble(5)),
                    rs.getString(6) == null ? "" : rs.getString(6),
                    rs.getString(7) == null ? "" : rs.getString(7)
                });
            }
        } catch (SQLException ex) {
            throw new Exception("Error BD (listarEstaciones): " + ex.getMessage());
        }
        return lista;
    }


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
                     "estado = CASE WHEN estaciones.fuera_servicio = 1 THEN 'FUERA_DE_SERVICIO' ELSE 'DISPONIBLE' END, " +
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

    // si la estación no existe se considera DESCONECTADA
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

    
    // cambia el estado y refresca la última conexión
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