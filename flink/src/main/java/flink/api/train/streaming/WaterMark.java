package flink.api.train.streaming;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WaterMark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStreamSource=env.socketTextStream("10.130.7.208",9999);

        SingleOutputStreamOperator<Object> da=dataStreamSource.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                String[] lines=value.split(" ");
                String key=lines[0];
                long values=Long.valueOf(lines[1]);
                return new Tuple2<String,Long>(key,values);
            }
        });
        // TODO: 2021/5/28  生成相应的key value 然后设置watermark 进行测试  实践体验 




    }
}
