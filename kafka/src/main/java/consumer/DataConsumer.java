package consumer;

import com.alibaba.fastjson.JSON;
import com.my.mysql.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.properties.util.PropertiesUtil;

import java.time.Duration;
import java.util.*;

import static com.alibaba.fastjson.JSON.parseArray;

public class DataConsumer {
    public Properties getProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.100.10:9092,192.168.100.11:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void comsumer() {
        Properties properties =PropertiesUtil.getConsumerProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        List<String> topics = new ArrayList<>();
        topics.add("my_topic_flink4");

        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
//
//                JSONObject jsonObject = JSON.parseObject(record.value());  如果接收到的一个对象里面的list  这里就需要解析，然后里面要get这个list  才是字符串
                System.out.println(record.value());
//                List<Order> list=JSON.parseArray(JSON.toJSONString(jsonObject.get("orderList")),Order.class);
                List<Order> list=JSON.parseArray(record.value(),Order.class);
//                jsonObject.get("orderList"),Map.class);
//                System.out.println(list.get(1).getDate());
//                List<List> mapList = parseArray(record.value(),List.class);
                for (Order stringObjectMap : list) {
                    System.out.println(stringObjectMap.getUserId()+"\t"+stringObjectMap.getUserName()+"\t"+stringObjectMap.getProductName()+"\t"+stringObjectMap.getDate());
//                    System.out.println(stringObjectMap.get("userId") + "\t"
//                            + stringObjectMap.get("userName") + "\t"
//                            + stringObjectMap.get("productName") + "\t"
//                            + stringObjectMap.get("productPrice") + "\t"
//                            + stringObjectMap.get("productType") + "\t"
//                            + stringObjectMap.get("manufacturers") + "\t"
//                            + stringObjectMap.get("date"));
                }
                System.out.printf("offset = %d, key = %s", record.offset(), record.key() );
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key() );
            }
        }
    }


    public static void main(String[] args) {
        DataConsumer dataConsumer = new DataConsumer();
        dataConsumer.comsumer();
    }
}
