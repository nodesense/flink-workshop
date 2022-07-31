package workshop.mysql;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class MysqlAccessWithCatalog {
    public static void main(String[] args) throws  Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        String name            = "my_catalog";
        String defaultDatabase = "test";
        String username        = "sammy";
        String password        = "PassWord@123";
        String baseUrl         = "jdbc:mysql://172.17.0.1:3306";

        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        tEnv.registerCatalog("my_catalog", catalog);

        // set the JdbcCatalog as the current catalog of the session
        tEnv.useCatalog("my_catalog");

        final Table result =  tEnv.sqlQuery("SELECT * FROM my_catalog.test.person");
        result.executeInsert("my_catalog.test.person2");
    }
}
