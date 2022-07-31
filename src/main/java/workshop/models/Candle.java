package workshop.models;

public class Candle {
    public  String asset;
    public java.sql.Timestamp st; // start time
    public java.sql.Timestamp et; // end time

    public Double O; // Open
    public Double C; // Close
    public Double H; // high
    public Double L; // low
    public  Double A; // Average (H + L + C) / 3
    public Double V; // volume
    public Double TA; // Turn Over Value/ TA Traded Amount

    public Double GapC; // from day close, based on LTP, candle close
    public Double Gap; // from day open, based on LTP, candle close
    public Double GapL; // from day low, based on LTP, candle close
    public Double GapH; // from day high, based on LTP, candle close

    public Double OI;
    public Double OIDiff;
    public Double OIGap;

    public String DT;  // IN/Index EQ FU/Future CE/Call PE/Put ETF
    public String UN; // NIFTY 50, NIFTY BANK etc or symbol applicable for FU, CE, PE

    public Integer N50 = 0; // is part of nifty 50
    public Integer N50T = 0; // Top  nifty 50 ~70%
    public Integer BNF = 0; // is part of BNF

    public Integer BNFT = 0; // top BNF 90%

    // GAP
    // DT IN EQ FU CE PE ETF
    // UN N50 BNF
    // N50 1/0 BNF 1/0


    @Override
    public String toString() {
        return "Candle{" +
                "asset='" + asset + '\'' +
                ", st=" + st +
                ", et=" + et +
                ", O=" + O +
                ", C=" + C +
                ", H=" + H +
                ", L=" + L +
                ", A=" + A +
                ", V=" + V +
                ", TA=" + TA +
                ", GapC=" + GapC +
                ", Gap=" + Gap +
                ", GapL=" + GapL +
                ", GapH=" + GapH +
                ", OI=" + OI +
                ", OIDiff=" + OIDiff +
                ", OIGap=" + OIGap +
                ", DT='" + DT + '\'' +
                ", UN='" + UN + '\'' +
                ", N50=" + N50 +
                ", BNF=" + BNF +
                '}';
    }
}
