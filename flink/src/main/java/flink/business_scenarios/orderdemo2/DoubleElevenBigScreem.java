package flink.business_scenarios.orderdemo2;

import com.google.inject.internal.cglib.core.$DuplicatesPredicate;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.util.Collector;
import org.apache.commons.lang3.time.FastDateFormat;

import java.util.List;

public class DoubleElevenBigScreem {
    private static class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double>{
        //初始化累加器
        @Override
        public Double createAccumulator() {
            return 0D;
        }

        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1+accumulator;  //就是在累加器的基础上 继续求和
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator; //返回一个累加器
        }

        @Override
        public Double merge(Double aDouble, Double acc1) {
            return aDouble+acc1;  //合并值
        }
    }


    public static class MyWindowFunction implements WindowFunction<Double, CategoryPojo, String, TimeWindow> {

        private FastDateFormat df = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        @Override
        //void apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out)
        public void apply(String s, TimeWindow timeWindow, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            long currentTimeMillis = System.currentTimeMillis();
            String dateTime = df.format(currentTimeMillis);
            Double totalPrice = input.iterator().next();
            out.collect(new CategoryPojo(s,totalPrice,dateTime));
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Double>> dataStreamSource = env.addSource(new MySource());

        DataStream<CategoryPojo> tempAggResult = dataStreamSource
                .keyBy(x -> x.f0)

                //*例如中国使用UTC+08:00，您需要一天大小的时间窗口，
                //*窗口从当地时间的00:00:00开始，您可以使用{@code of(时间.天(1),时间.hours(-8))}.
                //下面的代码表示从当天的00:00:00开始计算当天的数据,缺一个触发时机/触发间隔
                //3.1定义大小为一天的窗口,第二个参数表示中国使用的UTC+08:00时区比UTC时间早
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                //3.2自定义触发时机/触发间隔
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                //.sum()//简单聚合
                //3.3自定义聚合和结果收集
                //aggregate(AggregateFunction<T, ACC, V> aggFunction,WindowFunction<V, R, K, W> windowFunction)
                .aggregate(new PriceAggregate(), new MyWindowFunction());

            tempAggResult.print("初步聚合");


        //TODO 4.sink-使用上面初步聚合的结果(每隔1s聚合一下截止到当前时间的各个分类的销售总金额),实现业务需求:
            tempAggResult.keyBy(CategoryPojo::getDateTime)
                    .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                    .process(new ProcessWindowFunction<CategoryPojo, Object, String, TimeWindow>() {
                        @Override
                        public void process(String s, Context context, Iterable<CategoryPojo> input, Collector<Object> collector) throws Exception {

                        }

                    });

            env.execute();

    }


}
