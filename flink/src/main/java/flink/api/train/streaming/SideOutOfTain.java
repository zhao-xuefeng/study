package flink.api.train.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.java.tuple.Tuple3;


import java.util.ArrayList;
import java.util.List;

public class SideOutOfTain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment stenv=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<Integer,Integer,Integer>> ds=stenv.fromElements(new Tuple3<>(1,2,3),
                new Tuple3<>(1,2,4),new Tuple3<>(2,2,3),new Tuple3<>(3,2,3));

//        ds.filter(new FilterFunction<Tuple3<Integer, Integer, Integer>>() {
//            @Override
//            public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
//                return value._3()>3;
//            }
//        }).print();

//被弃用
//       SplitStream splitStream= ds.split(new OutputSelector<Tuple3<Integer, Integer, Integer>>() {
//            @Override
//            public Iterable<String> select(Tuple3<Integer, Integer, Integer> value) {
//                List<String> list=new ArrayList<>();
//                if (value._0()==1){
//                    list.add("oneStream");
//                }
//                if (value._0()==2){
//                    list.add("twoStream");
//                }
//                if (value._2()==3){
//                    list.add("threeStream");
//                }
//
//                return list;
//            }
//        });
//       splitStream.select("oneStream").print("oneStream");
//        splitStream.select("twoStream").print("twoStream");
//        splitStream.select("threeStream").print("threeStream");

/*推荐使用

定义 OutputTag
调用特定函数进行数据拆分
    ProcessFunction
    KeyedProcessFunction
    CoProcessFunction
    KeyedCoProcessFunction
    ProcessWindowFunction
    ProcessAllWindowFunction
* */
        OutputTag<Tuple3<Integer,Integer,Integer>> op=new OutputTag<Tuple3<Integer,Integer,Integer>>("oneStream"){};
        OutputTag<Tuple3<Integer,Integer,Integer>> op2=new OutputTag<Tuple3<Integer,Integer,Integer>>("twoStream"){};


        SingleOutputStreamOperator<Tuple3<Integer,Integer,Integer>> posscessTime=ds.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, Context context, Collector<Tuple3<Integer, Integer, Integer>> collector) throws Exception {
                if (value.f0==1){
                    context.output(op,value);
                }
                if (value.f0==2){
                    context.output(op2,value);
                }
            }
        });
        DataStream<Tuple3<Integer, Integer, Integer>> oneStream=posscessTime.getSideOutput(op);
        DataStream<Tuple3<Integer, Integer, Integer>> twoStream=posscessTime.getSideOutput(op2);
        oneStream.print("one");
        twoStream.print("two");
        stenv.execute();


    }
}
