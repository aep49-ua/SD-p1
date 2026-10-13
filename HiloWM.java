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
    
    public int sumar(int p_a, int p_b)
    {
        return p_a+p_b;
    }

    public int multiplicar(int p_a, int p_b)
    {
        return p_a*p_b;
    }

    public int realizarOperacion(String p_Cadena)
    {
        String[] operacion = p_Cadena.split(",");
        int res=0;
        
        System.out.println("SRV: La operacion es: " + operacion[0]);
        if(operacion.length != 1)
        {
            System.out.println("SRV: El operando 1 es " + operacion[1] + " y el operando 2 es " + operacion[2]);
            if(operacion[0].compareTo("suma")==0)
            {
                res = sumar(Integer.parseInt(operacion[1]),Integer.parseInt(operacion[2]));
            }
            else
            {
                if(operacion[0].compareTo("mult")==0)
                {
                    res = multiplicar(Integer.parseInt(operacion[1]),Integer.parseInt(operacion[2]));   
                }   
                else
                {
                    
                    res = -1;
                }
            }
            System.out.println("SRV: El resultado es: " + res);
        }else
        {
            res = -1;
        }   
        return (res);
    }
    
    
    
    @Override
    public void run() {
        String Cadena = "";
        String idEstacion = "Desconocida";

        try {
            while (true) {
                // Leer trama en bruto desde el socket
                Cadena = this.leeSocket(skCliente, "");

                if (Cadena == null || Cadena.trim().isEmpty()) {
                    System.out.println("SRV: Cliente " + idEstacion + " ha cerrado la conexión.");
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

                        mensajeRespuesta = "STATUS#OK#Estacion registrada correctamente";
                        System.out.println("SRV: Estación " + idEstacion + " (" + ubicacion + ") REGISTRADA/DADA DE ALTA.");
                    } else {
                        mensajeRespuesta = "STATUS#NO OK#Formato incorrecto para registro";
                    }
                }
                // Petición de LEAK / AVERÍA
                else if (comando.equals("LEAK") || comando.equals("AVERIA")) {
                    if (campos.length >= 2 && campos[1].startsWith("WS-")) {
                        idEstacion = campos[1].trim();

                        // Actualizar BD a LEAK
                        EstacionDAO.actualizarEstado(idEstacion, "LEAK");

                        mensajeRespuesta = "STATUS#OK#Averia registrada";
                        System.out.println("SRV: ALERTA - Fuga en estación " + idEstacion + " (Estado LEAK).");
                    } else {
                        mensajeRespuesta = "STATUS#NO OK#Formato incorrecto para averia";
                    }
                }

                // Paso 3: Empaquetar la respuesta con <STX><ANSWER><ETX><LRC> y enviarla
                String respuestaEmpaquetada = ProtocoloUtils.empaquetarTrama(mensajeRespuesta);
                this.escribeSocket(skCliente, respuestaEmpaquetada);
            }
        } 
        catch (Exception e) {
            System.out.println("SRV: Estación " + idEstacion + " DESCONECTADA súbitamente: " + e.getMessage());
            if (!idEstacion.equals("Desconocida")) {
                try {
                    EstacionDAO.actualizarEstado(idEstacion, "DESCONECTADA");
                } catch (Exception ex) {
                    System.err.println("Error al actualizar estado desconectado: " + ex.getMessage());
                }
            }
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