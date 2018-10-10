package com.onion.kafkastreamlrean.chapter4;

import com.onion.kafkastreamlrean.model.Purchase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;


/**
 * 以购物的时间为时间的基准
 */
public class TranscationTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previoudTimestamp) {
        Purchase purchase = (Purchase) consumerRecord.value();
        return purchase.getPurchaseDate().getTime();

    }
}
