package flinkWord;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class JavaWordcount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment=ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet=environment.fromElements("a b b c c c dd dd dd ddd","be be bebe dcd dcd dcd dcd dc  ac a");

        DataSet<Tuple2<String,Integer>> tuple2DataSet=dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String o:value.split(" ")){
                    out.collect(new Tuple2<>(o,1));
                }
            }
        });
        DataSet<Tuple2<String,Integer>> mapData=tuple2DataSet.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return value;
            }
        });
        UnsortedGrouping<Tuple2<String,Integer>> grouping=mapData.groupBy(0);
// 通过设置 并行度 发现 ，并行度太大  会导致有得分区数据为空，可以联想到kafka分区与flink并行度的设置
        AggregateOperator<Tuple2<String,Integer>> s=grouping.sum(1).setParallelism(8);
//
//        区内有序，若多个并行度 就是各个线程之内的有序
//        s.sortPartition(1,Order.DESCENDING).print("(********)");
        System.out.println("-----------------------------------");
        s.sortPartition(1, Order.DESCENDING).print();
    }
}
