package workshop.hive;
//
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.SqlDialect;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveCatalogTest {
    public static void main(String[] args) throws  Exception  {
//        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
//// to use hive dialect
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//// to use default dialect
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//
//        String name            = "myhive";
//        String defaultDatabase = "default";
//        String hiveConfDir     = "/opt/hive-conf";
//
//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
//        tableEnv.registerCatalog("myhive", hive);
//
//// set the HiveCatalog as the current catalog of the session
//        tableEnv.useCatalog("myhive");
    }
}
