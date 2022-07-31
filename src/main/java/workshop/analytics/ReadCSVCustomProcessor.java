package workshop.analytics;

import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ReadCSVCustomProcessor {
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public static void main(String[] args) throws  Exception {
        try {
            // set up the Java DataStream API
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // set up the Java Table API
            final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            //        tableEnv.getConfig().getConfiguration().setString("pipeline.jars", "file:///home/karthik/work/flink/jars/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar;file:///home/karthik/work/flink/jars/flink-sql-parquet_2.11-1.14.4.jar");

          //  536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2010-12-01 08:26:00,2.55,17850,United Kingdom

            Schema schema = Schema.newBuilder()
                    .column("f1", DataTypes.STRING())
                    .column("f2", DataTypes.STRING())
                    .column("f3", DataTypes.STRING())
                    .column("f4", DataTypes.STRING())
                    .column("f5", DataTypes.STRING())
                    .column("f6", DataTypes.STRING())
                    .column("f7", DataTypes.STRING())
                    .column("f8", DataTypes.STRING())
                    .build();

            //        register table for source
            TableDescriptor orcSrc = TableDescriptor.forConnector("filesystem")
                    .option(FileSystemConnectorOptions.PATH,"hdfs://localhost:9000/test/input/ecommerce-small.csv")
                    .option("format","csv")
                    .schema(schema)
                    .build();

            tableEnv.createTable("source", orcSrc);
            Table sourceTable = tableEnv.from("source");

            sourceTable.execute().print();

            //        register table for sink
            TableDescriptor orcSink = TableDescriptor.forConnector("filesystem")
                    .option(FileSystemConnectorOptions.PATH,"hdfs://localhost:9000/test/input/myoutput.csv")
                    .option("format","csv")
                    .schema(schema)
                    .build();

            tableEnv.createTable("sink", orcSink);
            Table sinkTable = tableEnv.from("sink");

            //         list tables
            tableEnv.executeSql("SHOW TABLES").print();

            //        inserting by tablePath
            sourceTable.printSchema();
            try{
                sourceTable.executeInsert(orcSink);
//                sourceTable.insertInto(orcSink).execute();
                if(sourceTable.hashCode() == sinkTable.hashCode()){
                    System.out.println("Entered HasCode");
                    if(sourceTable.equals(sinkTable)){
                        TableResult apiResult = sourceTable.insertInto("sink").printExplain().execute();
                        System.out.println("Insert into Sink");
                    }
                }
            }
            catch (Exception e){
                System.out.println("Issue with Insert Into"+e);
            }
//            System.out.println("Java API Job Status"+apiResult.getJobClient().get().getJobStatus());
        }
        catch (Exception e){
            System.out.println("Something went wrong.");
            System.out.println(e);
        }
    }
}
