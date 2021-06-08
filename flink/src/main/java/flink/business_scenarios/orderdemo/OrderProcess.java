package flink.business_scenarios.orderdemo;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class OrderProcess {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stenv=StreamTableEnvironment.create(env,envSettings);
        DataStreamSource<Order> orderDS  = env.addSource(new OrderSource());

        DataStream<Order> dataStreamWarterMark=orderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((order, timeStamp)->order.getCreateTime()));
        stenv.createTemporaryView("orderA",dataStreamWarterMark,$("orderId"),$("userId"),$("money"),$("createTime").rowtime());
//      既然是写sql，就一定是flinksql，需要去了解flinksql的一些函数的使用
        Table table=stenv.sqlQuery("select " +
                "userId,count(orderId) as cnt, " +
                "max(money) as  maxMoney," +
                "min(money) as minMoney " +
                "from orderA " +
                "group by userId, " +
                "tumble(createTime,INTERVAL '5' SECOND)"); //滚动窗口函数
        table.printSchema();
        DataStream<Tuple2<Boolean, Row>> dataStream=stenv.toRetractStream(table, Row.class);
        dataStream.print();
        env.execute();

    }
}
