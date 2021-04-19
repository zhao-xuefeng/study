package flinksql;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;


public class FlinkStreamTrainDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment=StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        DataStream<String> dataStream=environment.socketTextStream("localhost",9999);
//        DataStream<String>stringDataStream=dataStream.assignTimestampsAndWatermarks();
    }

}
