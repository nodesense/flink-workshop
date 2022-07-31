package workshop.models;

public class TrueDataTick {
    public  String Symbol;
    public  Long Symbol_ID;

    public java.sql.Timestamp Timestamp;

    public Double LTP;
    public Long LTQ;
    public Double ATP;
    public Long Volume;

    public Double Open;
    public Double High;
    public Double Low;
    public Double Prev_Close;

    public Long OI;
    public Long Prev_Open_Int_Close;

    public Double Day_Turnover;
    public String Special;
    public Long Tick_Sequence_No;

    public Double Bid;
    public Long Bid_Qty;

    public Double Ask;

    @Override
    public String toString() {
        return "TrueDataTick{" +
                "Symbol='" + Symbol + '\'' +
                ", Symbol_ID=" + Symbol_ID +
                ", Timestamp=" + Timestamp +
                ", LTP=" + LTP +
                ", LTQ=" + LTQ +
                ", ATP=" + ATP +
                ", Volume=" + Volume +
                ", Open=" + Open +
                ", High=" + High +
                ", Low=" + Low +
                ", Prev_Close=" + Prev_Close +
                ", OI=" + OI +
                ", Prev_Open_Int_Close=" + Prev_Open_Int_Close +
                ", Day_Turnover=" + Day_Turnover +
                ", Special='" + Special + '\'' +
                ", Tick_Sequence_No=" + Tick_Sequence_No +
                ", Bid=" + Bid +
                ", Bid_Qty=" + Bid_Qty +
                ", Ask=" + Ask +
                ", Ask_Qty=" + Ask_Qty +
                '}';
    }

    public Long Ask_Qty;
}
