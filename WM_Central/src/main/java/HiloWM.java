import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class HiloWM extends Thread {

    // socket de la estación a la que atiende este hilo
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
    
    // qué hilo atiende ahora a cada estación; así no se marca como desconectada una estación que ya se ha reconectado con otro hilo
    private static final java.util.Map<String, HiloWM> propietario = new java.util.concurrent.ConcurrentHashMap<>();

    // pone la estación en DESCONECTADA, pero solo si este hilo sigue siendo su propietario
    private void marcarDesconectada(String idEstacion) {
        if (idEstacion.equals("Desconocida")) {
            return;
        }
        // la estación ya se registró en otra conexión, no se toca
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

    // avisa al operario de que su riego ha terminado por una avería
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
            // si la estación no dice nada en 15 s se considera perdida la conexión
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
                        // si había un riego sin cerrar en la BD, la estación sigue contando como REGANDO
                        if (EstacionDAO.hayRiegoAbierto(idEstacion)) {
                            EstacionDAO.actualizarEstado(idEstacion, "REGANDO");
                        }

                        mensajeRespuesta = "STATUS#OK#Estacion registrada correctamente";
                        // si Central la había bloqueado antes de caerse, se le vuelve a mandar el bloqueo
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

                        // una avería corta el riego en curso: se cierra en el histórico y se avisa al operario
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
                // avería resuelta: vuelve a DISPONIBLE, o a FUERA_DE_SERVICIO si Central la había bloqueado
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
                // el ping solo sirve para comprobar que la conexión sigue viva
                else if (comando.equals("PING")) {
                    mensajeRespuesta = "STATUS#OK#pong";
                }

                // Paso 3: Empaquetar la respuesta con <STX><ANSWER><ETX><LRC> y enviarla
                String respuestaEmpaquetada = ProtocoloUtils.empaquetarTrama(mensajeRespuesta);
                this.escribeSocket(skCliente, respuestaEmpaquetada);

                // la estación confirma la respuesta; si en vez de eso manda ya otra trama, se guarda para el siguiente ciclo
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
        // pase lo que pase se cierra el socket
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