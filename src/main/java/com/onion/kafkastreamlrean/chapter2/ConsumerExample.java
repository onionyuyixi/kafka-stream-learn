package com.onion.kafkastreamlrean.chapter2;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerExample {

    private volatile boolean doneConsuming = false;
    private int numberPartitions ;
    private ExecutorService executorService;

    public ConsumerExample(int numberPartitions) {
        this.numberPartitions = numberPartitions;
    }

    private void startConsuming(){
         executorService = Executors.newFixedThreadPool(numberPartitions);

        for (int i = 0; i <numberPartitions ; i++) {
            Runnable runnable = getConsumerThread(getProperties());
            executorService.submit(runnable);
        }
    }

    private Runnable getConsumerThread(Properties properties) {
        return ()->{
            Consumer<String,String> consumer = null;
            try {
                consumer = new KafkaConsumer<String, String>(properties);
                consumer.subscribe(Collections.singletonList("chapter2"));
                while (!doneConsuming){
                    ConsumerRecords<String, String> records = consumer.poll(1);
                    for (ConsumerRecord<String, String> record : records) {
                        String message = String.format("ConsumeRecord: key = %s value = %s with offset = %d partition = %d",
                                record.key(), record.value(), record.offset(), record.partition());
                        consumer.commitAsync();
                        System.out.println(message);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
                consumer.commitSync();
            }finally {
                if(consumer!=null){
                    consumer.close();
                }
            }
        };
    }

    public void stopConsuming() throws InterruptedException {
        doneConsuming = true;
        executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
        executorService.shutdownNow();
    }

    private Properties getProperties(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id", "consumer-example");
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "3000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return  properties;
    }
    public static void main(String[] args) throws InterruptedException {
        ConsumerExample consumerExample = new ConsumerExample(100);
        consumerExample.startConsuming();
        Thread.sleep(6000*60);
        consumerExample.stopConsuming();

    }
}
