import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class HiloWM extends Thread {

    private Socket skCliente;
    
    public HiloWM(Socket p_cliente)
    {
        this.skCliente = p_cliente;
    }
    
    /*
    * Lee datos del socket. Supone que se le pasa un buffer con hueco 
    *   suficiente para los datos. Devuelve el numero de bytes leidos o
    * 0 si se cierra fichero o -1 si hay error.
    */
    public String leeSocket (Socket p_sk, String p_Datos)
    {
        try
        {
            InputStream aux = p_sk.getInputStream();
            DataInputStream flujo = new DataInputStream( aux );
            p_Datos = new String();
            p_Datos = flujo.readUTF();
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e.toString());
        }
      return p_Datos;
    }

    /*
    * Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
    * o -1 si hay error.
    */
    public void escribeSocket (Socket p_sk, String p_Datos)
    {
        try
        {
            OutputStream aux = p_sk.getOutputStream();
            DataOutputStream flujo= new DataOutputStream( aux );
            flujo.writeUTF(p_Datos);      
        }
        catch (Exception e)
        {
            System.out.println("Error: " + e.toString());
        }
        return;
    }
    
    private static final java.util.Map<String, HiloWM> propietario = new java.util.concurrent.ConcurrentHashMap<>();

    private void marcarDesconectada(String idEstacion) {
        if (idEstacion.equals("Desconocida")) {
            return;
        }
        if (propietario.get(idEstacion) != this) {
            return;
        }
        propietario.remove(idEstacion);
        try {
            EstacionDAO.actualizarEstado(idEstacion, "DESCONECTADA");
            EventosCentral.add(idEstacion + " -> estación desconectada");
        } catch (Exception ex) {
            System.err.println("Error al actualizar estado desconectado: " + ex.getMessage());
        }
    }

    private void notificarFinPorAveria(String operario, String idEstacion, String motivo) {
        if (WM_Central.producer != null) {
            WM_Central.producer.notificarOperario(operario, idEstacion, "FINISHED", motivo);
        }
    }

    @Override
    public void run() {
        String Cadena = "";
        String idEstacion = "Desconocida";
        String pendiente = null;

        try {
            skCliente.setSoTimeout(15000);
            while (true) {
                // Leer trama en bruto desde el socket
                if (pendiente != null) {
                    Cadena = pendiente;
                    pendiente = null;
                } else {
                    Cadena = this.leeSocket(skCliente, "");
                }

                if (Cadena == null || Cadena.trim().isEmpty()) {
                    System.out.println("SRV: Cliente " + idEstacion + " ha cerrado la conexión.");
                    marcarDesconectada(idEstacion);
                    break;
                }

                // Paso 1: Desempaquetar <STX><DATA><ETX><LRC> y validar LRC
                String datosValidados = ProtocoloUtils.desempaquetarYValidar(Cadena);

                if (datosValidados == null) {
                    // Si el LRC o la trama no son válidos, se responde NACK
                    System.err.println("SRV: Trama corrupta o LRC inválido recibido.");
                    this.escribeSocket(skCliente, ProtocoloUtils.NACK);
                    continue; // Vuelve a esperar una trama válida
                }

                // Responder ACK confirmando la recepción correcta de la petición
                this.escribeSocket(skCliente, ProtocoloUtils.ACK);

                // Paso 2: Procesar los campos del <DATA> (separados por #)
                String[] campos = datosValidados.split("#");
                String comando = campos[0];
                String mensajeRespuesta = "STATUS#NO OK#Comando no reconocido";

                // Petición de REGISTRO / ALTA
                if (comando.equals("REGISTRO") || comando.equals("ALTA")) {
                    if (campos.length == 3 && campos[1].startsWith("WS-") && !campos[2].trim().isEmpty()) {
                        idEstacion = campos[1].trim();
                        String ubicacion = campos[2].trim();

                        // Persistencia en SQLite
                        EstacionDAO.registrarOAltaEstacion(idEstacion, ubicacion);
                        propietario.put(idEstacion, this);
                        if (EstacionDAO.hayRiegoAbierto(idEstacion)) {
                            EstacionDAO.actualizarEstado(idEstacion, "REGANDO");
                        }

                        mensajeRespuesta = "STATUS#OK#Estacion registrada correctamente";
                        if (EstacionDAO.estaFueraDeServicio(idEstacion)) {
                            if (WM_Central.producer != null) {
                                WM_Central.producer.send("wm-comandos", "BLOCK#" + idEstacion);
                            }
                            EventosCentral.add(idEstacion + " (" + ubicacion + ") registrada. Sigue Fuera de Servicio");
                        } else {
                            EventosCentral.add(idEstacion + " (" + ubicacion + ") registrada. Estación DISPONIBLE");
                        }
                        System.out.println("SRV: Estación " + idEstacion + " (" + ubicacion + ") REGISTRADA/DADA DE ALTA.");
                    } else {
                        mensajeRespuesta = "STATUS#NO OK#Formato incorrecto para registro";
                    }
                }
                // Petición de LEAK / AVERÍA
                else if (comando.equals("LEAK") || comando.equals("AVERIA")) {
                    if (campos.length >= 2 && campos[1].startsWith("WS-")) {
                        idEstacion = campos[1].trim();

                        String operario = EstacionDAO.finalizarRiego(idEstacion, "INTERRUMPIDO_LEAK",
                                EstacionDAO.obtenerVolumen(idEstacion), -1);
                        if (operario != null) {
                            notificarFinPorAveria(operario, idEstacion, "Riego interrumpido por fuga");
                        }

                        // Actualizar BD a LEAK
                        EstacionDAO.actualizarEstado(idEstacion, "LEAK");

                        mensajeRespuesta = "STATUS#OK#Averia registrada";
                        EventosCentral.add(idEstacion + " -> fuga detectada. Riego bloqueado");
                        System.out.println("SRV: ALERTA - Fuga en estación " + idEstacion + " (Estado LEAK).");
                    } else {
                        mensajeRespuesta = "STATUS#NO OK#Formato incorrecto para averia";
                    }
                }
                else if (comando.equals("RESUELTA")) {
                    if (campos.length >= 2 && campos[1].startsWith("WS-")) {
                        idEstacion = campos[1].trim();
                        String destino = EstacionDAO.estaFueraDeServicio(idEstacion) ? "FUERA_DE_SERVICIO" : "DISPONIBLE";
                        if (EstacionDAO.actualizarEstadoSi(idEstacion, destino, "LEAK")) {
                            EventosCentral.add(idEstacion + " -> avería resuelta. Estación " + destino);
                            System.out.println("SRV: Estación " + idEstacion + " recuperada (" + destino + ").");
                        }
                        mensajeRespuesta = "STATUS#OK#Averia resuelta";
                    } else {
                        mensajeRespuesta = "STATUS#NO OK#Formato incorrecto para resolucion";
                    }
                }
                else if (comando.equals("PING")) {
                    mensajeRespuesta = "STATUS#OK#pong";
                }

                // Paso 3: Empaquetar la respuesta con <STX><ANSWER><ETX><LRC> y enviarla
                String respuestaEmpaquetada = ProtocoloUtils.empaquetarTrama(mensajeRespuesta);
                this.escribeSocket(skCliente, respuestaEmpaquetada);

                String confirmacion = this.leeSocket(skCliente, "");
                if (confirmacion != null && !confirmacion.isEmpty()
                        && confirmacion.charAt(0) == ProtocoloUtils.STX) {
                    pendiente = confirmacion;
                } else if (confirmacion == null || confirmacion.isEmpty()) {
                    System.out.println("SRV: Cliente " + idEstacion + " ha cerrado la conexión.");
                    marcarDesconectada(idEstacion);
                    break;
                }
            }
        }
        catch (Exception e) {
            System.out.println("SRV: Estación " + idEstacion + " DESCONECTADA súbitamente: " + e.getMessage());
            marcarDesconectada(idEstacion);
        }
        finally {
            try {
                if (skCliente != null && !skCliente.isClosed()) {
                    skCliente.close();
                    System.out.println("SRV: Socket liberado para " + idEstacion);
                }
            } catch (Exception e) {
                System.out.println("SRV: Error al cerrar el socket: " + e.getMessage());
            }
        }
    }
}