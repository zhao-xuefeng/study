package flink.tablesql;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableWordcount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment  env=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stenv=StreamTableEnvironment.create(env,envSetting);
        DataStream<Word> dataStream=env.fromElements(new Word("hello",1),
                new Word("hello",1),
                new Word("hello",1),
                new Word("hello",1));

        Table table=stenv.fromDataStream(dataStream);
/*
 Table result=table.groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency")) 会报frequency 字段找不到

* 在java api里面as是返回了EXPR$0, 而不是（word, frequency），as （"frequency"）只是定义了table schema。
* toRetractStream的直接输入是（word， EXPR$0,）  。
* 在select后面跟个.as("word", "frequency") 转一下就好了
*
* */
        Table result=table.groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency")).as("word","frequency");
        DataStream<Tuple2<Boolean,Word>> tuple2DataStream=stenv.toRetractStream(result,flink.tablesql.Word.class);
        tuple2DataStream.print();
        env.execute();



    }
}
