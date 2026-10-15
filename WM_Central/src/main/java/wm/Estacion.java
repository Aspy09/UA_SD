package wm;

// Datos de una estacion de riego tal y como los conoce la Central
public class Estacion {
    private final String id;
    private String ubicacion;
    private EstadoWS estado;
    private boolean bloqueada; // bloqueada por CENTRAL: se mantiene aunque se desconecte y vuelva

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

    public void setUbicacion(String ubicacion) { this.ubicacion = ubicacion; }
    public void setEstado(EstadoWS estado) { this.estado = estado; }
    public void setBloqueada(boolean bloqueada) { this.bloqueada = bloqueada; }

    @Override
    public String toString() {
        return id + " (" + ubicacion + ") -> " + estado;
    }
}
