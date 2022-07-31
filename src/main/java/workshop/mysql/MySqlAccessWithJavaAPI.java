package workshop.mysql;

import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlAccessWithJavaAPI {
    public static void main(String[] args) throws  Exception {
//        StreamExecutionEnvironment env = new StreamExecutionEnvironment().getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .inStreamingMode()
//                .build();
        StreamTableEnvironment t_env = StreamTableEnvironment.create(env);
        try {
            Schema schema = Schema.newBuilder()
                    .column("PersonID", DataTypes.INT())
                    .column("Name", DataTypes.STRING())
                    .build();

            //        register table for source
            TableDescriptor sourceTableDescriptor = TableDescriptor.forConnector("jdbc")
                    .option(JdbcConnectorOptions.URL, "jdbc:mysql://172.17.0.1:3306/test")
                    .option(JdbcConnectorOptions.USERNAME, "root")
                    .option(JdbcConnectorOptions.PASSWORD, "root")
                    .option(JdbcConnectorOptions.TABLE_NAME, "person")
                    .schema(schema)
                    .build();

            t_env.createTable("source", sourceTableDescriptor);
            t_env.from("source").printSchema();

            //        register table for sink
            TableDescriptor sinkTableDescriptor = TableDescriptor.forConnector("jdbc")
                    .option(JdbcConnectorOptions.URL, "jdbc:mysql://172.17.0.1:3306/test")
                    .option(JdbcConnectorOptions.USERNAME, "root")
                    .option(JdbcConnectorOptions.PASSWORD, "root")
                    .option(JdbcConnectorOptions.TABLE_NAME, "person2")
                    .schema(schema)
                    .build();

            t_env.createTable("sink", sinkTableDescriptor);
            t_env.from("sink").printSchema();

            //        show table
            t_env.executeSql("SHOW TABLES").print();

            //        inserting by tablePath
            Table result = t_env.from("source");
            result.printSchema();
            TableResult javResult = result.insertInto("sink").execute();
            System.out.println("Java API Job Status"+javResult.getJobClient().get().getJobStatus());

        }
        catch (Exception e){
            System.out.println("Something went wrong");
        }
    }
}
