package workshop.table;

import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.models.Sector;
import workshop.util.SqlText;

// https://www1.nseindia.com/content/equities/EQUITY_L.csv

public class S034_FlinkCSVExample {
    public static void main(String[] args) throws  Exception  {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API on top of data stream
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Company Name,Industry,Symbol,Series,ISIN Code
        String csvSql = SqlText.getSQL("/sql/SectorsCSV.sql");
        System.out.println(csvSql);

        tableEnv.executeSql(csvSql);

        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT *, 'NIFTY' as index  FROM Sectors");

        result.printSchema();

        result.execute().print();

        // Flink support data stream to table, table to data stream in easier way..
        Table sectorsTable = tableEnv.sqlQuery("SELECT * from Sectors");
        // print table schema
        sectorsTable.printSchema();
        // now get a data stream from table
        // Sector.class is java reflection, meta class, used for instantation, casting purpose
        DataStream<Sector> sectorStream = tableEnv.toDataStream(sectorsTable, Sector.class);



        sectorStream.print();

        // we can also create table/temp view from data stream
        // this code can extract schema from pojo class itself
        Table sector2 = tableEnv.fromDataStream(sectorStream);
        System.out.println("Sector 2 schema");
        sector2.printSchema();
        // create temp view [temp view available for this flink client session]
        // temp view won't create table in catalogs ie is not permanent table
        tableEnv.createTemporaryView("Sectors2", sector2);

        Table t = tableEnv.sqlQuery("SELECT industry,asset from Sectors2 where industry='Power' ");

        t.printSchema();
        t.execute().print();

        String plan =  t.explain(ExplainDetail.CHANGELOG_MODE);

        System.out.println("PLAN " + plan);
          env.execute();

    }
}