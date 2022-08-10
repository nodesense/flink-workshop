package workshop.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.models.Product;
import workshop.models.Sector;
import workshop.util.SqlText;




// kafka-avro-console-producer --broker-list localhost:9092 --topic products --property value.schema='{"type":"record","name":"product","fields":[{"name":"id","type":"int"},{"name":"name", "type": "string"}, {"name":"price", "type": "int"}]}'  --property schema.registry.url="http://localhost:8081"

/* data for producer , DO NOT ADD EMPTY LINES
{"id":11,"name":"phone2","price":111}
{"id":12,"name":"phone2","price":222}
{"id":13,"name":"phone2","price":333}
{"id":14,"name":"phone444","price":444}
 */

//kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic products --from-beginning --property schema.registry.url="http://localhost:8081"

// Run FlinkAvroExamplejava



public class S036_FlinkAvro {
    public static void main(String[] args) throws  Exception  {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API on top of data stream
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Company Name,Industry,Symbol,Series,ISIN Code
        String csvSql = SqlText.getSQL("/sql/ProductAvro.sql");
        System.out.println(csvSql);

        tableEnv.executeSql(csvSql);

        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT *   FROM products");

        result.printSchema();

        result.execute().print();


        DataStream<Product> productStream = tableEnv.toDataStream(result, Product.class);

        productStream.print();

        env.execute();

    }
}