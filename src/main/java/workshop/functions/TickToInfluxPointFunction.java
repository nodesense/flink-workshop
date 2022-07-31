package workshop.functions;

// import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;


public class TickToInfluxPointFunction {}
//
//public class TickToInfluxPointFunction extends RichMapFunction<TickTA, InfluxDBPoint> {
//
//    @Override
//    public InfluxDBPoint map(TickTA tick) throws Exception {
//
//        String measurement = tick.asset;
//        long timestamp =tick.ts.getTime();
//
//        HashMap<String, String> tags = new HashMap<>();
//        //tags.put("host", input[1]);
//        //tags.put("region", "region#" + String.valueOf(input[1].hashCode() % 20));
//
//        HashMap<String, Object> fields = new HashMap<>();
//
//        for (String name: tick.fields.keySet()) {
//            fields.put(name, tick.fields.get(name));
//        }
//
//        return new InfluxDBPoint(measurement, timestamp, tags, fields);
//    }
//}
