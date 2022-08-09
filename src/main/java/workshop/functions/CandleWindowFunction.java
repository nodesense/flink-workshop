package workshop.functions;

import workshop.models.TrueDataTick;
import workshop.models.Candle;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class CandleWindowFunction extends  ProcessWindowFunction<TrueDataTick, Candle, String, TimeWindow> {

    private ValueStateDescriptor<Candle> candleStateDescriptor = new ValueStateDescriptor("CandleState",Candle.class);
    private ValueStateDescriptor<TrueDataTick> lastTickStateDescriptor = new ValueStateDescriptor("LastTickState",TrueDataTick.class);

    public CandleWindowFunction(String name) {

    }

    private static final long serialVersionUID = 1L;

    ArrayList<String> Nifty50List = new ArrayList<String>(
            Arrays.asList("ADANIPORTS",
            "ASIANPAINT",
            "AXISBANK",
            "BAJAJ-AUTO",
            "BAJAJFINSV",
            "BAJFINANCE",
            "BHARTIARTL",
            "BPCL",
            "BRITANNIA",
            "CIPLA",
            "COALINDIA",
            "DIVISLAB",
            "DRREDDY",
            "EICHERMOT",
            "GAIL",
            "GRASIM",
            "HCLTECH",
            "HDFC",
            "HDFCBANK",
            "HDFCLIFE",
            "HEROMOTOCO",
            "HINDALCO",
            "HINDUNILVR",
            "ICICIBANK",
            "INDUSINDBK",
            "INFY",
            "IOC",
            "ITC",
            "JSWSTEEL",
            "KOTAKBANK",
            "LT",
            "M&M",
             "M-M", // same as M&M workaround
            "MARUTI",
            "NESTLEIND",
            "NTPC",
            "ONGC",
            "POWERGRID",
            "RELIANCE",
            "SBILIFE",
            "SBIN",
            "SHREECEM",
            "SUNPHARMA",
            "TATAMOTORS",
            "TATASTEEL",
            "TCS",
            "TECHM",
            "TITAN",
            "ULTRACEMCO",
            "UPL",
            "WIPRO",
            "TATACONSUM"
                    ));

    ArrayList<String>  niftyBanks = new ArrayList<String>(
            Arrays.asList(
            "AUBANK",
            "AXISBANK",
            "BANDHANBNK",
            "FEDERALBNK",
            "HDFCBANK",
            "ICICIBANK",
            "IDFCFIRSTB",
            "INDUSINDBK",
            "KOTAKBANK",
            "PNB",
            "RBLBANK",
            "SBIN"
            ));

    ArrayList<String>  niftyBankAlphas = new ArrayList<String>(
            Arrays.asList(
                    "HDFCBANK", // ~27
                    "ICICIBANK", // ~23
                    "SBIN", //~12
                    "AXISBANK", //~12
                    "KOTAKBANK", //~11
                    "INDUSINDBK" // ~5
            ));


    ArrayList<String> Nifty50Alphas = new ArrayList<String>(
            Arrays.asList(
                    "RELIANCE",
                    "INFY",
                    "KOTAKBANK",
                    "HDFCBANK",
                    "ICICIBANK",
                    "HDFC",
                    "TCS",
                    "ITC",
                    "HINDUNILVR",
                    "LT",
                    "AXISBANK",
                    "BHARTIARTL",
                    "BAJFINANCE",
                    "ASIANPAINT",
                    "MARUTI",
                    "HCL"
            ));

    @Override
    public void process(String key, Context context, Iterable<TrueDataTick> iterable, Collector<Candle> collector) throws Exception {

   //      System.out.println("Iterator Begin " + new Date());

        // Context.windowState is per window level, keyed, within window like thumling window
        ValueState<Candle> candleState = context.windowState().getState(candleStateDescriptor);
        // globalState is global level, still keyed
        ValueState<TrueDataTick> lastTickState = context.globalState().getState(lastTickStateDescriptor);

        Candle candle = candleState.value();

        TrueDataTick lastTick = lastTickState.value();


        for(TrueDataTick tick : iterable){

            if (candle == null) {
                candle = new Candle();
                candle.asset = tick.Symbol;
                candle.st = tick.Timestamp;
                candle.et = tick.Timestamp;
                candle.O = tick.LTP;
                candle.C = tick.LTP;
                candle.H = tick.LTP;
                candle.L = tick.LTP;
                candle.V = 0.0;
                candle.TA = 0.0;
            }

            candle.H = Double.max(candle.H, tick.LTP);
            candle.L = Double.min(candle.L, tick.LTP);
            candle.C = tick.LTP;
            candle.et = tick.Timestamp;

            if (lastTick != null) {
                double volume = tick.Volume - lastTick.Volume;
                if (volume >= 0) {
                    candle.V += volume;
                } else {
                    // TODO ??
                }

                double amount = tick.Day_Turnover - lastTick.Day_Turnover;
                if (amount >= 0) {
                    candle.TA += amount;
                } else {
                    // TODO ??
                }

                candle.OIDiff = (double) (tick.OI - lastTick.OI);
            }

            candle.Gap =  tick.LTP - tick.Open;
            candle.GapC = tick.LTP - tick.Prev_Close;
            candle.GapH = tick.LTP - tick.High;
            candle.GapL = tick.LTP - tick.Low;
            candle.OIGap = (double) (tick.OI - tick.Prev_Open_Int_Close);
            candle.OI = tick.OI.doubleValue();

            lastTick = tick;
        }

        if (lastTick != null) {
            lastTickState.update(lastTick);
        }



        if (candle != null) {

            if (candle.OIDiff == null)
                candle.OIDiff = 0.0;

            if (candle.asset.startsWith("NIFTY") && (candle.asset.endsWith("-I") || candle.asset.endsWith("-II") || candle.asset.endsWith("-III") )) {
                candle.DT = "FU";
                candle.UN = "NIFTY 50";
            }

            if (candle.asset.startsWith("BANKNIFTY") && (candle.asset.endsWith("-I") || candle.asset.endsWith("-II") || candle.asset.endsWith("-III") )) {
                candle.DT = "FU";
                candle.UN = "NIFTY BANK";
            }

            if (candle.asset.startsWith("NIFTY") && candle.asset.endsWith("CE")) {
                candle.DT = "CE";
                candle.UN = "NIFTY 50";
            }

            if (candle.asset.startsWith("BANKNIFTY") && candle.asset.endsWith("CE")) {
                candle.DT = "CE";
                candle.UN = "NIFTY BANK";
            }

            if (candle.asset.startsWith("NIFTY") && candle.asset.endsWith("PE")) {
                candle.DT = "PE";
                candle.UN = "NIFTY 50";
            }

            if (candle.asset.startsWith("BANKNIFTY") && candle.asset.endsWith("PE")) {
                candle.DT = "PE";
                candle.UN = "NIFTY BANK";
            }

            if (niftyBanks.contains(candle.asset)) {
                candle.BNF = 1;
                candle.DT = "EQ";
            }

            if (Nifty50List.contains(candle.asset)) {
                candle.N50 = 1;
                candle.DT = "EQ";
            }

            if ( (candle.asset.equals("NIFTY 50")) || (candle.asset.equals("NIFTY BANK"))) {
                candle.DT = "IN";
            }


            if (niftyBankAlphas.contains(candle.asset)) {
                candle.BNFT = 1;
            }

            if (Nifty50Alphas.contains(candle.asset)) {
                candle.N50T = 1;
            }

            if (candle.DT == null) {
                candle.DT = "OT";
            } //TODO: Index check

            candle.A = (candle.H + candle.C + candle.L) / 3.0;

            candleState.update(candle);
            collector.collect(candle);
        }
    }
}