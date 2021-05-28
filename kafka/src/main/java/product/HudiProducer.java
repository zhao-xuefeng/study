
package product;
import com.alibaba.fastjson.JSON;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import product.Hudi;


import java.sql.SQLException;

import java.util.*;

public class HudiProducer {
    public Properties getProperties(){
        Properties properties=new Properties();
        properties.put("bootstrap.servers","10.130.7.207:9092");
        properties.put("ack","all");
        properties.put("liner.ms",1);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
    public  void productData(Object o,String hudiTopic) throws SQLException {
        Properties properties=getProperties();
        KafkaProducer<String, String> producer=new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<String, String>(hudiTopic, JSON.toJSONString(o)));
        producer.close();
    }

    public static void main(String[] args) throws SQLException {
        HudiProducer hudiProducer=new HudiProducer();
        List<String> countries=new ArrayList<>();
        countries.add("China");
        countries.add("Canada");
        countries.add("Japan");
        countries.add("India");
        countries.add("Germany");
//        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("");
//        System.out.println();

        for (int i=1;i<=10;i++) {
            Hudi hudi = new Hudi();
            String uuid = UUID.randomUUID().toString().replace("-", "");
            hudi.setUuid(uuid);
            hudi.setClose(i * new Random().nextDouble() * 10);
            hudi.setKey(new Random().toString());
            hudi.setDay("2020-3-" + String.valueOf(1 + i));
            hudi.setHigh(i * new Random().nextDouble() * 10 + 3);
            hudi.setMonth(i);
            hudi.setCountry(countries.get(new Random().nextInt(5)));
            hudi.setTs(System.currentTimeMillis() + 2);
            hudi.setLow(i * new Random().nextDouble() * 5);
            hudi.setOpen(i * new Random().nextDouble() * 3);
            hudi.setSymbol("this is demo");
            hudi.setYear(2021);
            hudi.set_hoodie_is_deleted(false);
//            hudiProducer.productData(hudi, args[0]);
            System.out.println(JSON.toJSONString(hudi));
        }
    }
}
