package workshop.stream;



import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.util.SqlText;

public class KafkaAvroCustomProcessor {
    public static void main(String[] args) throws  Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String jsonSql = SqlText.getSQL("/sql/InvoiceKafkaAvro.sql");
        System.out.println(jsonSql);

        tableEnv.executeSql(jsonSql);


        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT * FROM Invoices");

        result.execute().print();

//        // convert the Table back to an insert-only DataStream of type `Order`
//        DataStream<Invoice> dataValueStream = tableEnv.toDataStream(result, Invoice.class);
////
//       dataValueStream.print();
////
//        // Long??
//        WatermarkStrategy< Invoice> watermarkStrategy =  WatermarkStrategy
//                .<Invoice>forBoundedOutOfOrderness(Duration.ofSeconds(60))
//                .withTimestampAssigner((event, timestamp) -> event.getInvoiceDate());
//
//        DataStream<Invoice>  dataValueStreamWithWaterMark = dataValueStream
//                .assignTimestampsAndWatermarks(watermarkStrategy);
//
//
//        final StreamingFileSink<Invoice> fileSink = StreamingFileSink
//                .forRowFormat(new Path("outputs/test"), new SimpleStringEncoder<Invoice>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                                .withMaxPartSize(1024 * 1024 * 1024)
//                                .build())
//                .build();
//

        // after the table program is converted to a DataStream program,
        // we must use `env.execute()` to submit the job
        env.execute();

    }
}