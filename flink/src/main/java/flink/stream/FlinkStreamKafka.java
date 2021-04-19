package flink.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.my.mysql.Order;
import com.properties.util.PropertiesUtil;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class FlinkStreamKafka {
    public static void main(String[] args) throws Exception {
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment stenv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(stenv, settings);
        stenv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("my_topic_flink4",
                new SimpleStringSchema(),
                PropertiesUtil.getConsumerProperties());
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1000)));

        DataStreamSource<String> dataStream = stenv.addSource(consumer);

        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name = "hive";
        String defaultDatabase = "myflink";
        String hiveConfDir = "flink/src/main/resources/";

        Catalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(name, hive);


//        tableEnv.executeSql("create table order_db (userId string ,userName string,productName string,producrdate string) ").print();


        dataStream.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                List<Order> list = JSON.parseArray(s, Order.class);
                for (Order order : list) {
                    String sql="insert into order_db(userId ,userName,productName,productdate)values(" + order.getUserId() + "," + order.getUserName() + "," + order.getProductName()
                            + "," + order.getDate() + ")";
                    System.out.println(sql);
                    tableEnv.executeSql(sql).print();
                }
                tableEnv.executeSql("select * from order_db").print();
                return "";
            }
        }).addSink(new SinkFunction<Object>() {
            @Override
            public void invoke(Object value, Context context) throws Exception {

            }
        });
        stenv.execute("flinkkafkastart");


    }
}
