package com.properties.util;

import java.util.Properties;

public class PropertiesUtil {
    public static Properties getConsumerProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.100.10:9092,192.168.100.11:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
