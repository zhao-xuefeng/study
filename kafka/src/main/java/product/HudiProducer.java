package product;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.SQLException;
import java.util.*;

public class HudiProducer {
        public Properties getProperties(){
            Properties properties=new Properties();
            properties.put("bootstrap.servers","192.168.100.10:9092,192.168.100.11:9092");
            properties.put("ack","all");
            properties.put("reties",1);
            properties.put("liner.ms",1);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            return properties;
        }
        public void productData(Object o) throws SQLException {
            Properties properties=getProperties();
            KafkaProducer<String, String> producer=new KafkaProducer<>(properties);
            producer.send(new ProducerRecord<String, String>("hudi_on_flink", JSON.toJSONString(o)));

            producer.close();


        }

        public static void main(String[] args) throws SQLException {
            Student student=new Student();
            List<Map<String,Object>> list=new ArrayList<>();
            for (int i=1;i<=10;i++){
                Map<String,Object> map=new HashMap<>();
                student.setId(i);
                student.setName("xiaoming"+i);
                student.setAddress("beijing"+i);
                student.setScore(i*60.5);
                map.put("id",student.getId());
                map.put("name",student.getName());
                map.put("address",student.getAddress());
                map.put("score",student.getScore());
                list.add(map);
            }

            HudiProducer hudiProducer=new HudiProducer();
            hudiProducer.productData(list);

        }


}
