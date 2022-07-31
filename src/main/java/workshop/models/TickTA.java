package workshop.models;

import org.apache.flink.table.annotation.DataTypeHint;

import java.util.HashMap;
import java.util.Map;

public class TickTA {
    public  String asset;
    public java.sql.Timestamp ts;

    @Override
    public String toString() {
        return "TickTA{" +
                "asset='" + asset + '\'' +
                ", ts=" + ts +
                ", fields=" + fields +
                '}';
    }

    @DataTypeHint("MAP<STRING, DOUBLE>") public Map<String,Double> fields = new HashMap<>();

    public double getField(String name) {
        if (fields.containsKey(name)) {
            return fields.get(name);
        }

        return Double.NaN;
    }
}
