package com.onion.kafkastreamlrean.chapter4;

import com.onion.kafkastreamlrean.model.Purchase;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * 按着客户id进行分区
 */
public class RewardsStreamPartitioner implements StreamPartitioner<String, Purchase> {


    @Override
    public Integer partition(String key, Purchase value, int numPartitions) {
        return value.getCustomerId().hashCode() % numPartitions;
    }
}
