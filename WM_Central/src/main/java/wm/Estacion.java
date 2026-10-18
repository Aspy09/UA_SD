package wm;

// datos de una estacion de riego en la central
public class Estacion {
    private final String id;
    private String ubicacion;
    private EstadoWS estado;
    private boolean bloqueada; // bloqueada por la central, se guarda aunque se desconecte

    // datos del riego para el panel
    private String operario; // quien pidio el riego, null si esta libre
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
    public EstadoWS getEstado() { return estado; }
    public boolean isBloqueada() { return bloqueada; }
    public String getOperario() { return operario; }
    public double getCaudal() { return caudal; }
    public double getVolumen() { return volumen; }

    public void setUbicacion(String ubicacion) { this.ubicacion = ubicacion; }
    public void setEstado(EstadoWS estado) { this.estado = estado; }
    public void setBloqueada(boolean bloqueada) { this.bloqueada = bloqueada; }
    public void setOperario(String operario) { this.operario = operario; }
    public void setCaudal(double caudal) { this.caudal = caudal; }
    public void setVolumen(double volumen) { this.volumen = volumen; }

    @Override
    public String toString() {
        return id + " (" + ubicacion + ") -> " + estado;
    }
}
