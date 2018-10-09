package com.onion.kafkastreamlrean.chapter2;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

public class MyPartitioner extends DefaultPartitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Object newKey = null;
        if(key!=null){
            newKey = (String) key;
//            newKey = purchaseKey.getConsumerId();
            keyBytes = ((String) newKey).getBytes();
        }
        return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
    }
}
