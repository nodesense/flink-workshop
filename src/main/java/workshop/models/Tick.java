package workshop.models;

import java.io.Serializable;

public class Tick implements Serializable {

    public  String asset;
    public java.sql.Timestamp ts;


    public Double LTP;
    public Double ATP;
    public Double Vol;
    public Double AP;
    public Double BP;
    public Double AQ;
    public Double BQ;
    public Double OI;
    public  Double DTO  = -1.0;


    @Override
    public String toString() {
        return "DataValue{" +
                "asset='" + asset + '\'' +
                ", ts=" + ts +
                ", LTP=" + LTP +
                ", ATP=" + ATP +
                ", Vol=" + Vol +
                ", AP=" + AP +
                ", BP=" + BP +
                ", AQ=" + AQ +
                ", BQ=" + BQ +
                ", OI=" + OI +
                '}';
    }
}
