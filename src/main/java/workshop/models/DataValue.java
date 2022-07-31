package workshop.models;


public class DataValue {

    public  String asset;
    public String name;
    public java.sql.Timestamp ts;
    public String tag;
    public Double value;

    // defines a TIMESTAMP data type of millisecond precision with an explicit conversion class
    //public @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class) java.sql.Timestamp new_time;

    //public java.time.LocalDateTime new_time;



    @Override
    public String toString() {
        return "DataValue{" +
                "asset='" + asset + '\'' +
                ", name='" + name + '\'' +
                ", tag='" + tag + '\'' +
                ", value=" + value +
                ", ts=" + ts +
                //    ", new_time=" + new_time +
                '}';
    }


}
