package workshop.hive;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import workshop.util.SqlText;

public class HiveCatalogReferenceApp {
    public static void main(String[] args) throws  Exception  {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
// to use hive dialect
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
// to use default dialect
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String name            = "moviecat"; // myhive
        String defaultDatabase = "mymovies"; //default, create database mymovies;
        String hiveConfDir     = "/opt/hive-conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("moviecat", hive);

// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("moviecat"); // use the tables from mymovies db

        // NOTE: we are not loading SQL/CREATE TABLE as we already done it in HiveCatalogTest


        // now this application, can reference table from hive catalog,
        // we no need to create again and again..

        Table t = tableEnv.from("MoviesHiveHadoop");
        t.printSchema();
        t.execute().print();

        // here we don't use hive sql, instead we use flink batch to run analytics
        Table t2 = tableEnv.sqlQuery("SELECT movieId, title from MoviesHiveHadoop where title LIKE 'T%' ");
        t2.printSchema();
        t2.execute().print();


    }
}
