package ru;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MyKafkaProducer {
    public static Properties props = new Properties();

    {
        String brokerList = System.getProperty("kafka.brokerList");

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    }

    public static Producer<String, String> producer = new KafkaProducer<>(props);

    public static void sendToKafka() {
        producer.send(new ProducerRecord<String, String>("alerts", "keyError", "valueError"));
        producer.close();
    }

}
