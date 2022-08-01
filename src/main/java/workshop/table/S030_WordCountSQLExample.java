package workshop.table;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/** The famous word count example that shows a minimal Flink SQL job in batch execution mode. */
public final class S030_WordCountSQLExample {

    public static void main(String[] args) throws Exception {

        // set up the Table API
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        // execute a Flink SQL job and print the result locally
       TableResult result =  tableEnv.executeSql(
                        // define the aggregation
                        "SELECT word, SUM(frequency) AS `count`\n"
                                // read from an artificial fixed-size table with rows and columns
                                + "FROM (\n"
                                + "  VALUES ('Hello', 1), ('Ciao', 1), ('Hello', 2)\n"
                                + ")\n"
                                // name the table and its columns
                                + "AS WordTable(word, frequency)\n"
                                // group for aggregation
                                + "GROUP BY word");
       result.print();

        String plan = tableEnv.explainSql(
                        // define the aggregation
                        "SELECT word, SUM(frequency) AS `count`\n"
                                // read from an artificial fixed-size table with rows and columns
                                + "FROM (\n"
                                + "  VALUES ('Hello', 1), ('Ciao', 1), ('Hello', 2)\n"
                                + ")\n"
                                // name the table and its columns
                                + "AS WordTable(word, frequency)\n"
                                // group for aggregation
                                + "GROUP BY word") ;

        System.out.println("SQL EXPLAIN " + plan);

    }
}