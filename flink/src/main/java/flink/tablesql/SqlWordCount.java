package flink.tablesql;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class SqlWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment  stenv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSetting= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment env=StreamTableEnvironment.create(stenv,envSetting);
        DataStream<Word>  dataStream=stenv.fromCollection(Arrays.asList(new Word("hello",1),
                new Word("hello",1),
                new Word("hello",2)));

        env.createTemporaryView("word_tb",dataStream,$("word"),$("frequency"));

        Table tableA=env.fromDataStream(dataStream);
        Table  resultTableA=tableA.groupBy($("word")).select($("word"),$("frequency").sum().as("frequency"));
        Table resultTable=env.sqlQuery("select word,sum(frequency) as frequency from word_tb group by word");
         resultTable.printSchema();
         resultTableA.printSchema();
         DataStream<Tuple2<Boolean, Word> > dataStream1=env.toRetractStream(resultTableA,Word.class);
         dataStream1.print();

         stenv.execute();


    }
}
