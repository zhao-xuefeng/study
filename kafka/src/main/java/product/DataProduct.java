package product;

import com.alibaba.fastjson.JSON;
import com.my.mysql.DataUtil;
import com.my.mysql.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.my.mysql.GenerateData;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DataProduct {
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
    public void productData() throws SQLException {
        Properties properties=getProperties();
        List<Order> order=new GenerateData().getData();
        KafkaProducer<String, String> producer=new KafkaProducer<>(properties);

        producer.send(new ProducerRecord<String, String>("my_topic_flink4", JSON.toJSONString(order)));
        producer.close();

    }

    public static void main(String[] args) throws SQLException {
        DataProduct dataProduct=new DataProduct();
        dataProduct.productData();

    }
}
