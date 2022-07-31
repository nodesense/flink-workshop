package workshop.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

public class S042_OnceFileProcessing {
    public static void main(String[] args) throws  Exception {
// create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set the restart strategy.
        //env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(NO_OF_RETRIES, 0));
        // env.enableCheckpointing(10);
        // create and start the file creating thread.

        // create the monitoring source along with the necessary readers.
        String localFsURI = "/home/krish/IdeaProjects/FlinkDemo/data/orders-stream";
        TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path(localFsURI));
        // format.setCharsetName(charsetName);

        format.setFilesFilter(FilePathFilter.createDefaultFilter());

        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;


        DataStream<String> inputStream = env.readFile(format, localFsURI,
                FileProcessingMode.PROCESS_ONCE, -1, typeInfo);

        PrintSinkFunction<String> sink = new PrintSinkFunction<>();

        //  tickTAStream.addSink(printFunction);


        inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                System.out.println(" VALUE " + value);
                out.collect(value);
            }
        }).addSink(sink).setParallelism(1);


        env.execute();
    }
}
