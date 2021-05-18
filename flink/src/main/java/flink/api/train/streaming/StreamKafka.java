package flink.api.train.streaming;


import com.properties.util.PropertiesUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;



public class StreamKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment stenv = StreamExecutionEnvironment.getExecutionEnvironment();
//
        DataStreamSource<ObjectNode> dataStreamSource = stenv.addSource(new FlinkKafkaConsumer<>("topic_1",
                new JSONKeyValueDeserializationSchema(true),
                PropertiesUtil.getConsumerProperties()));
//如果是objetdata 类型的 则输出字段是
//         {"value":{"name":"nihao","age":12,"day":"20210518"},"metadata":{"offset":2,"topic":"topic_1","partition":0}}
        dataStreamSource.map(new MapFunction<ObjectNode, Object>() {
            @Override
//            此处要想打印是要根据反回类型来决定的，如果类型不匹配会导致空指针异常
            public Object map(ObjectNode jsonNodes) throws Exception {
                    Object day="日期："+jsonNodes.get("value").get("day")+"    "+"partition:"+jsonNodes.get("metadata").get("partition");
//                   System.out.println("日期："+day+"  "+"topic:"+jsonNodes.get("metadata").get("topic").asText()+"  "+"partition:"+jsonNodes.get("metadata").get("partition ").asText());
                    return day;
            }
        }).print();
//

//        DataStreamSource<String> dataStreamSource1=stenv.addSource(new FlinkKafkaConsumer<String>("topic_2",new SimpleStringSchema(),PropertiesUtil.getConsumerProperties()));
//        dataStreamSource1.map(new MapFunction<String, Object>() {
//            @Override
//            public Object map(String s) throws Exception {
//
//                return s;
//            }
//        }).print();
        stenv.execute();
    }
}
