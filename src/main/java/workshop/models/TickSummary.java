package workshop.models;

public class TickSummary {
    public  String asset;
    public java.sql.Timestamp ts;

    public  double gain = -100;

    @Override
    public String toString() {
        return "TickSummary{" +
                "asset='" + asset + '\'' +
                ", ts=" + ts +
                ", gain=" + gain +
                ", volume=" + volume +
                ", amount=" + amount +
                '}';
    }

    public double volume = 0;
    public double amount = 0;
}
