package workshop.models;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DataValueTimeAssigner extends  BoundedOutOfOrdernessTimestampExtractor<DataValue> {
    DataValue dataValue;

    public DataValueTimeAssigner() {
        super(Time.seconds(5));
    }

    @Override
    public long extractTimestamp(DataValue dataValue) {
        return dataValue.ts.getTime();
    }

}
