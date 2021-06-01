package flink.api.train.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;


public class Windows {
    public static void main(String[] args) throws Exception {
/*
* 此示例是错误的 无法运行，缺少时间处理
*
*
*
* */

        StreamExecutionEnvironment stenv=StreamExecutionEnvironment.getExecutionEnvironment();
        stenv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);DataStreamSource<String> dataStreamSource=stenv.socketTextStream("10.130.7.208",9999);
        SingleOutputStreamOperator<Tuple2<String,Long>>streamOperator=dataStreamSource.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String,Long> map(String value) throws Exception {
               String[] lines= value.split(",");
                return new Tuple2<String,Long>(lines[0],Long.valueOf(lines[1]));
            }
        });
       streamOperator.keyBy(x->x.f0).window(TumblingEventTimeWindows.of(Time.seconds(3l))).apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
           SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
           @Override
           public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
               List<Tuple2<String, Long>> list=new ArrayList<>();
               input.forEach(new Consumer<Tuple2<String, Long>>() {
                   @Override
                   public void accept(Tuple2<String, Long> stringLongTuple2) {

                       list.add(stringLongTuple2);
                       System.out.println("输出数据是："+stringLongTuple2.f1.toString()+"|"+format.format(stringLongTuple2.f1));
                   }
               });
               out.collect(s+"|"+window.getStart()+"|"+window.getEnd()+"|"+list.size());
           }
       }).print();

       stenv.execute();
    }
}
