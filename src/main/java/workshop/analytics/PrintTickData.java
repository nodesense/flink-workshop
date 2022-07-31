package workshop.analytics;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.util.SqlText;

public class PrintTickData {
    public static void main(String[] args) throws  Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set up the Java Table API
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String jsonSql = SqlText.getSQL("/sql/TickSourceJson.sql");
        System.out.println(jsonSql);

        tableEnv.executeSql(jsonSql);

        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT LTP FROM Ticks where `asset` = 'NIFTY 50'");

         result.execute().print();
    }
}
