package flink.api.train.streaming;


import org.apache.flink.api.common.eventtime.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;


public class WaterMark2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("10.130.7.208", 9999);

        SingleOutputStreamOperator<Tuple2<String, Long>> da = dataStreamSource.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] lines = value.split(",");
                String key = lines[0];
                long values = Long.valueOf(lines[1]);
                return new Tuple2<String, Long>(key, values);
            }
        });
/*
* 最新的写法
* */
        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarks2= da.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.f1));

        KeyedStream<Tuple2<String, Long>, String> ds = waterMarks2.keyBy(x -> x.f0);
        WindowedStream ws = ds.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        ws.apply(new WindowFunction<Tuple2<String,Long>, Tuple7<String,String, Integer,String,String,String,String>,String, TimeWindow >() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple7<String,String, Integer, String, String, String, String>> out) throws Exception {
                List<Tuple2<String, Long>> list=new ArrayList<>();
                input.forEach(new Consumer<Tuple2<String, Long>>() {
                    @Override
                    public void accept(Tuple2<String, Long> stringLongTuple2) {

                        list.add(stringLongTuple2);
                        System.out.println("输出数据是："+stringLongTuple2.f1.toString()+"|"+format.format(stringLongTuple2.f1));
                    }
                });
                long listF1=list.get(0).f1;
                long listF2=list.get(list.size()-1).f1;
                out.collect(new Tuple7<>("触发：",s,list.size(),format.format(listF1),format.format(listF2),format.format(window.getStart()),format.format(window.getEnd())));
            }
        }).print();
        env.execute();



//        da.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5l)));
//        da.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5l)).withTimestampAssigner(new TimestampAssignerSupplier<Object>() {
//            @Override
//            public TimestampAssigner<Object> createTimestampAssigner(Context context) {
//                return null;
//            }
//        }))
//        博文链接
          //https://blog.csdn.net/sxiaobei/article/details/81147723?utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-1.control&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-1.control

    }
}
