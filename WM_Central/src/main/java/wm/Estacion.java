package wm;

// datos de una estacion de riego en la central
public class Estacion {
    private final String id;
    private String ubicacion;
    private String region = "ALICANTE"; // provincia, decide a que topic de ordenes se le mandan
    private EstadoWS estado;
    private boolean bloqueada; // bloqueada por la central, se guarda aunque se desconecte

    // datos del riego para el panel
    private String operario; // quien pidio el riego, null si esta libre
    private String operacion; // numero del riego que esta haciendo (OP-0007), para la trazabilidad
    private long reservadaDesde = 0; // cuando se reservo (ms), para cancelarla si la estacion no contesta
    private double caudal;   // litros por minuto
    private double volumen;  // litros que lleva

    public Estacion(String id, String ubicacion) {
        this.id = id;
        this.ubicacion = ubicacion;
        this.estado = EstadoWS.DESCONECTADA;
        this.bloqueada = false;
    }

    public String getId() { return id; }
    public String getUbicacion() { return ubicacion; }
    public String getRegion() { return region; }
    public EstadoWS getEstado() { return estado; }
    public boolean isBloqueada() { return bloqueada; }
    public String getOperario() { return operario; }
    public String getOperacion() { return operacion; }
    public long getReservadaDesde() { return reservadaDesde; }
    public double getCaudal() { return caudal; }
    public double getVolumen() { return volumen; }

    public void setUbicacion(String ubicacion) { this.ubicacion = ubicacion; }
    public void setRegion(String region) { this.region = region; }
    public void setEstado(EstadoWS estado) { this.estado = estado; }
    public void setBloqueada(boolean bloqueada) { this.bloqueada = bloqueada; }
    public void setOperario(String operario) { this.operario = operario; }
    public void setOperacion(String operacion) { this.operacion = operacion; }
    public void setReservadaDesde(long reservadaDesde) { this.reservadaDesde = reservadaDesde; }
    public void setCaudal(double caudal) { this.caudal = caudal; }
    public void setVolumen(double volumen) { this.volumen = volumen; }

    @Override
    public String toString() {
        return id + " (" + ubicacion + ") -> " + estado;
    }
}
