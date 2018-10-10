package com.onion.kafkastreamlrean.chapter5;

import com.onion.kafkastreamlrean.model.StockTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Date;

/**
 * 开始使用table
 */
public class StockTransactionTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        if(!(consumerRecord.value() instanceof StockTransaction)){
            return System.currentTimeMillis();
        }

        StockTransaction stockTransaction = (StockTransaction) consumerRecord.value();
        Date transactionTimestamp = stockTransaction.getTransactionTimestamp();
        return transactionTimestamp!=null ? transactionTimestamp.getTime() : consumerRecord.timestamp();


    }
}
