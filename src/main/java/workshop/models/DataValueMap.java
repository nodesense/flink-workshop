package workshop.models;

import org.apache.flink.table.annotation.DataTypeHint;

import java.util.HashMap;
import java.util.Map;

public class DataValueMap {
    public  String asset;

    public java.sql.Timestamp ts;

    public double wave1;
    public double wave2;
    public double wave3;
    public double wave4;
    public double wave5;
    public double wave6;
    public double wave7;
    public double wave8;
    public double wave9;
    public double wave10;
    public double wave11;
    public double wave12;
    public double wave13;
    public double wave14;
    public double wave15;
    public double wave16;
    public double wave17;
    public double wave18;
    public double wave19;
    public double wave20;
    public double wave21;
    public double wave22;
    public double wave23;
    public double wave24;


    public double signal;
    public double adiff;
    public double sdiff;

    public double williamr;
    public double cci;
    public double rsi;

    public double kcm;
    public double kcu;
    public double kcl;

    public double roc;

    public double ltp;
    public double arup; // aroon up
    public double ardown; // aroon down
    public double aroc; // aron ocilitrator
    public double lreg; // least sqauore


    @Override
    public String toString() {
        return "DataValueMap{" +
                "asset='" + asset + '\'' +
                ", ts=" + ts +
                ", wave1=" + wave1 +
                ", wave2=" + wave2 +
                ", wave3=" + wave3 +
                ", wave4=" + wave4 +
                ", wave5=" + wave5 +
                ", wave6=" + wave6 +
                ", wave7=" + wave7 +
                ", wave8=" + wave8 +
                ", wave9=" + wave9 +
                ", wave10=" + wave10 +
                ", wave11=" + wave11 +
                ", wave12=" + wave12 +
                ", wave13=" + wave13 +
                ", wave14=" + wave14 +
                ", wave15=" + wave15 +
                ", wave16=" + wave16 +
                ", wave17=" + wave17 +
                ", wave18=" + wave18 +
                ", wave19=" + wave19 +
                ", wave20=" + wave20 +
                ", wave21=" + wave21 +
                ", wave22=" + wave22 +
                ", wave23=" + wave23 +
                ", wave24=" + wave24 +
                ", signal=" + signal +
                ", adiff=" + adiff +
                ", sdiff=" + sdiff +
                '}';
    }

    //@DataTypeHint("MAP<STRING, DOUBLE>") public Map<String,Double> fields = new HashMap<>();
}
