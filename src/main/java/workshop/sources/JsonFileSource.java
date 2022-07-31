package workshop.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import workshop.util.SqlText;

import java.util.Properties;

public class  JsonFileSource<T> {
    protected StreamExecutionEnvironment env = null;
    protected StreamTableEnvironment tableEnv = null;

    // Issue, need to follow https://gist.github.com/yunspace/930d4d40a787a1f6a7d1
//        Class<T> persistentClass = (Class<T>) ((ParameterizedType) getClass()
//                .getGenericSuperclass()).getActualTypeArguments()[0];

    public JsonFileSource( StreamExecutionEnvironment env,  StreamTableEnvironment tableEnv ) {
        this.env = env;
        this.tableEnv = tableEnv;
    }

    public DataStream<T> load(Properties properties) throws  Exception {

        Class c = (Class) properties.get("ClassType");

        String jsonSql = SqlText.getSQL("/sql/TickSourceJson.sql");
        System.out.println(jsonSql);

        tableEnv.executeSql(jsonSql);

        // union the two tables
        final Table result =  tableEnv.sqlQuery("SELECT * FROM Ticks");
        //result.execute().print();

        // convert the Table back to an insert-only DataStream of type `Order`
        DataStream<T> tickDataStream = tableEnv.toDataStream(result, c);
        return tickDataStream;
    }
}