package workshop.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.models.Sector;
import workshop.util.SqlText;

public class S035_MovieLensAnalytics {
    public static void main(String[] args) throws  Exception  {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API on top of data stream
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Company Name,Industry,Symbol,Series,ISIN Code
        String movieSql = SqlText.getSQL("/sql/Movies.sql");
        System.out.println(movieSql);
        tableEnv.executeSql(movieSql);

        String ratingSql = SqlText.getSQL("/sql/Ratings.sql");
        System.out.println(ratingSql);
        tableEnv.executeSql(ratingSql);

        // union the two tables
        final Table movieTable =  tableEnv.sqlQuery("SELECT *  FROM Movies");

        movieTable.printSchema();

        movieTable.execute().print();

        final Table ratingsTable =  tableEnv.sqlQuery("SELECT *  FROM Ratings");

        ratingsTable.printSchema();

        ratingsTable.execute().print();

         env.execute();

    }
}