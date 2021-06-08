package flink.business_scenarios.orderdemo2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.util.Collector;

public class DoubleElevenBigScreem {
    public static void main(String[] args) {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Double>> dataStreamSource=env.addSource(new MySource());

        dataStreamSource.keyBy(x->x.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Double>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Double> stringDoubleTuple2, Context context, Collector<Object> collector) throws Exception {

                    }
                });
    }
}
