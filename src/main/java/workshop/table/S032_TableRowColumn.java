package workshop.table;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDate;

import static org.apache.flink.table.api.Expressions.*;

public final class S032_TableRowColumn {

    public static void main(String[] args) throws Exception {

        // setup the unified API
        // in this case: declare that the table programs should be executed in batch mode
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);

        // Mutual fund holding
        // fundname, fund type, date invested, amount invested, isDebt fund
        // folio no,  account_id, invested, avgnav, current_nav, units,
        // calculate xirr, current price
        // transactions folio_number,   date, days, amount, nav, unit, , find profit and loss from given dates from current nav

        // create a table with example data without a connector required
        final Table mutualFundPortpolio =
                env.fromValues(
                        Row.of(
                                "111234343", // foliono
                                LocalDate.parse("2020-06-16"), // date invested
                                1000.0, // amount spend
                                50, // unit acquired
                                20.0 // nav per unit at the time of purchase
                        ),
                        Row.of(
                                "111234343", // foliono
                                LocalDate.parse("2020-08-18"), // date invested
                                2000.0, // amount spend
                                80, // unit acquired
                                25.0 // nav per unit at the time of purchase
                        ),
                        Row.of(
                                "23252432423", // foliono
                                LocalDate.parse("2021-04-14"), // date invested
                                500.0, // amount spend
                                50, // unit acquired
                                10.0 // nav per unit at the time of purchase
                        ),
                        Row.of(
                                "23252432423", // foliono
                                LocalDate.parse("2021-11-19"), // date invested
                                3000.0, // amount spend
                                200, // unit acquired
                                15.0 // nav per unit at the time of purchase
                        )
                );

        // handle ranges of columns easily
        final Table withTableSelect = mutualFundPortpolio.select(withColumns(range(1, 5)));
        withTableSelect.printSchema();
        withTableSelect.execute().print();


        // name columns
        final Table namedColumns =
                withTableSelect.as(
                        "folio_no",
                        "invested_date",
                        "amount",
                        "units",
                        "nav");

        // register a view temporarily
        env.createTemporaryView("mutual_funds", namedColumns);

        namedColumns.printSchema();
        namedColumns.execute().print();

        env.sqlQuery(
                        "SELECT "
                                + "  COUNT(*) AS `number of investments`, "
                                + "  AVG(YEAR(invested_date)) AS `average  year` "
                                + "FROM `mutual_funds`")
                .execute()
                .print();

        final Table javaQueryTable =
                env.from("mutual_funds")
                        .filter($("folio_no").isNotNull())
                        .filter($("amount").isGreaterOrEqual(1000))
                        .filter($("invested_date").isGreaterOrEqual(LocalDate.parse("2021-01-01")))
                        .select(
                                $("folio_no").upperCase().as("folio_no"),
                                $("amount"),
                                $("invested_date"));

        javaQueryTable.printSchema();
        javaQueryTable.execute().print();
    }
}