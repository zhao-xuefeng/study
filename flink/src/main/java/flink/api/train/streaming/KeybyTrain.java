package flink.api.train.streaming;

import flinksql.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class KeybyTrain {

    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;
        int count1=0;
        int count2=0;
        int count3=0;
        int count4=0;
        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }


//     update 方法
//    可访问的运算符状态更新为给定值。下一次
//
//*{@link#value（）}被调用（对于相同的状态分区），返回的状态将表示
//
//*更新后的值。当分区状态更新为null时，当前
//
//*键将被删除，并在下次访问时返回默认值


        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {

            System.out.println("flatmap1执行前的状态："+blocked.value()+"----"+ "执行了第"+String.valueOf(count1++)+"次");
            blocked.update(Boolean.TRUE);
            System.out.println("flatmap1输出的："+control_value);

            System.out.println("flatmap1执行后的状态："+blocked.value()+"++++"+"执行了第"+String.valueOf(count2++)+"次");
            System.out.println("---------------------------------------------------------");

        }


        //flatmap1  与flatmap2
        // 每个元素顺序比较若在1里面的出现过得元素且在2里面比这个元素的位置靠后或等于才会改变flatmap2的状态，
        // 也可以理解为 这时候的key状态更新
        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {

            System.out.println("flatmap2执行前的状态："+blocked.value()+"********"+ "执行了第"+String.valueOf(count3++)+"次");
            if (blocked.value() == null) {
                System.out.println("flatmap2输出的："+data_value);
                out.collect(data_value);
            }
            System.out.println("flatmap2执行后的状态："+blocked.value()+"~~~~~~~~"+"执行了第"+String.valueOf(count4++)+"次");

            System.out.println("=======================================");


        }
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment stenv=StreamExecutionEnvironment.getExecutionEnvironment();
//

        DataStream<String> control = stenv
                .fromElements("cccc","cccc","Apache","DROP", "IGNORE","aaaa")
                .keyBy(x -> x);
//        control.print("----------");

        DataStream<String> streamOfWords = stenv
                .fromElements( "IGNORE" ,"aaaaa","DROP","Apache",  "Flink")
                .keyBy(x -> x);
//        streamOfWords.print("=========");

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print("程序运行结果为：");

        System.out.println(stenv.getParallelism());
        stenv.execute();
    }
}
