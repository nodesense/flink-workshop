package workshop.mysql;

import org.apache.flink.connector.jdbc.table.JdbcConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySqlAccessWithSqlQueries {
    public static void main(String[] args) throws  Exception {
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
                    .option(JdbcConnectorOptions.USERNAME, "sammy")
                    .option(JdbcConnectorOptions.PASSWORD, "PassWord@123")
                    .option(JdbcConnectorOptions.TABLE_NAME, "person")
                    .schema(schema)
                    .build();

            t_env.createTable("source", sourceTableDescriptor);
            t_env.from("source").printSchema();

            //        register table for sink
            TableDescriptor sinkTableDescriptor = TableDescriptor.forConnector("jdbc")
                    .option(JdbcConnectorOptions.URL, "jdbc:mysql://172.17.0.1:3306/test")
                    .option(JdbcConnectorOptions.USERNAME, "sammy")
                    .option(JdbcConnectorOptions.PASSWORD, "PassWord@123")
                    .option(JdbcConnectorOptions.TABLE_NAME, "person2")
                    .schema(schema)
                    .build();

            t_env.createTable("sink", sinkTableDescriptor);
            t_env.from("sink").printSchema();

            //        show table
            t_env.executeSql("SHOW TABLES").print();

            //        inserting by executeSql.
            //        executeSql will execute single query.
            TableResult sqlResult = t_env.executeSql("insert into sink select * from source");
            System.out.println("Sql Job Status" + sqlResult.getJobClient().get().getJobStatus());

            //        inserting by StatementSet.
            //        StatementSet will execute a set of query.
            StatementSet statSet = t_env.createStatementSet();
            statSet.addInsertSql("insert into sink select * from source");
            TableResult statResult = statSet.execute();
            // get job status through TableResult
            System.out.println("Statement Job Status" + statResult.getJobClient().get().getJobStatus());
        }
        catch (Exception e) {
            System.out.println("Something went wrong");
        }
    }
}
