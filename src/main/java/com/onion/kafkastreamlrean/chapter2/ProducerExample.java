package com.onion.kafkastreamlrean.chapter2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

public class ProducerExample {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("compression.type", "snappy");
        properties.put("partitioner.class", MyPartitioner.class.getName());

        PurchaseKey key = new PurchaseKey("12334568", new Date());

        try (Producer<String, String> producer = new KafkaProducer<String, String>(properties)) {

            long l = System.currentTimeMillis();
            for (int i = 0; i < 1000000; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("chapter2", key.getConsumerId() + "--" + i, "value");
                producer.send(record, ((metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                }));
            }
            System.err.println(System.currentTimeMillis() - l);


        }
    }
}
