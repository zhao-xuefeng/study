package flink.api.train.streaming;


import org.apache.flink.api.common.eventtime.*;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
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


public class WaterMarkLater {
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

        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarks = da.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            private Watermark watermark = null;
            private long maxOutOfOrderness = 5000L; //最大允许的乱序时间是5s
            private long currentMaxTimestamp = Long.MIN_VALUE + maxOutOfOrderness + 1;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
//                long waterMarks = Long.valueOf(watermark.getTimestamp());
//                String waterMakesDate = waterMarks > -5000l ? format.format(waterMarks) : watermark.toString();
//                System.out.println("timestamp:" + element.f0 + "," + element.f1 + "|" +"当前时间："+ format.format(element.f1) + "," + currentMaxTimestamp + "|" + "当前最大时间："+format.format(currentMaxTimestamp) + "," + watermark.toString()+"----");

                /*watermark  输出这样输出会导致空指针异常那个不知道先执行哪个
                * */
                System.out.println("timestamp:" + element.f0 + "," + element.f1 + "|" + "当前时间：" + format.format(element.f1) + "," + currentMaxTimestamp + "|" + "当前最大时间：" + format.format(currentMaxTimestamp) + "," + watermark.toString() );

                return timestamp;
            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1);
            }
        });

//        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarks2= da.assignTimestampsAndWatermarks(WatermarkStrategy
//                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
//                .withTimestampAssigner((event, timestamp) -> event.f1));
        KeyedStream<Tuple2<String, Long>, String> ds = waterMarks.keyBy(x -> x.f0);
        WindowedStream ws = ds.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        OutputTag<Tuple2<String, Long>> sideOutput = new OutputTag<Tuple2<String, Long>>("laterData"){};

        SingleOutputStreamOperator<String> singleOutputStreamOperator = ws
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(sideOutput).
                        apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                            @Override
                            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                                List<Tuple2<String, Long>> list = new ArrayList<>();
                                input.forEach(new Consumer<Tuple2<String, Long>>() {
                                    @Override
                                    public void accept(Tuple2<String, Long> stringLongTuple2) {

                                        list.add(stringLongTuple2);
                                        System.out.println("输出数据是：" + stringLongTuple2.f1.toString() + "|" + format.format(stringLongTuple2.f1));
                                    }
                                });
                                long listF1 = list.get(0).f1;
                                long listF2 = list.get(list.size() - 1).f1;
                                out.collect("触发："+ s+ list.size()+ format.format(listF1)+ format.format(listF2)+ format.format(window.getStart())+format.format(window.getEnd()));
                            }
                        });
        singleOutputStreamOperator.print("正常的的数据");

//        正常的的数据:2> 触发：152021-06-02 09:52:50.6792021-06-02 09:52:54.6792021-06-02 09:52:50.0002021-06-02 09:52:55.000
//        timestamp:1,1622598787679|当前时间：2021-06-02 09:53:07.679,1622598787679|当前最大时间：2021-06-02 09:53:07.679,Watermark @ 1622598778678
//        timestamp:1,1622598761679|当前时间：2021-06-02 09:52:41.679,1622598784679|当前最大时间：2021-06-02 09:53:04.679,Watermark @ 1622598779678
//        timestamp:1,1622598763679|当前时间：2021-06-02 09:52:43.679,1622598786679|当前最大时间：2021-06-02 09:53:06.679,Watermark @ 1622598781678
//        timestamp:1,1622598762679|当前时间：2021-06-02 09:52:42.679,1622598785679|当前最大时间：2021-06-02 09:53:05.679,Watermark @ 1622598780678
//        丢失的数据:2> (1,1622598763679)
//        丢失的数据:2> (1,1622598761679)
//        丢失的数据:2> (1,1622598762679)
//        timestamp:1,1622598764679|当前时间：2021-06-02 09:52:44.679,1622598787679|当前最大时间：2021-06-02 09:53:07.679,Watermark @ 1622598782678
//        丢失的数据:2> (1,1622598764679)

        DataStream<Tuple2<String, Long>> lateData = singleOutputStreamOperator
                .getSideOutput(sideOutput);
        lateData.print("丢失的数据");
        env.execute();


//        博文链接
        //https://blog.csdn.net/sxiaobei/article/details/81147723?utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-1.control&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7EBlogCommendFromMachineLearnPai2%7Edefault-1.control


    }
}
