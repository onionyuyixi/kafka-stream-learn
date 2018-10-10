package com.onion.kafkastreamlrean.chapter5;

import com.onion.kafkastreamlrean.clients.MockDataProducer;
import com.onion.kafkastreamlrean.collectors.FixedSizePriorityQueue;
import com.onion.kafkastreamlrean.model.ShareVolume;
import com.onion.kafkastreamlrean.model.StockTransaction;
import com.onion.kafkastreamlrean.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import static com.onion.kafkastreamlrean.clients.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class AggregateAndReduceExample {

    private static Logger LOG= LoggerFactory.getLogger(AggregateAndReduceExample.class);

    public static void main(String[] args) {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.ShareVolumeSerde();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.FixedSizePriorityQueueSerde();

        NumberFormat numberFormat = NumberFormat.getInstance();

        Comparator<ShareVolume> comparator = (sv1,sv2)->sv2.getShares()-sv1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedQueue = new FixedSizePriorityQueue<>(comparator, 5);

        ValueMapper<FixedSizePriorityQueue,String> valueMapper = fpq->{
            StringBuilder stringBuilder = new StringBuilder();
            Iterator<ShareVolume> iterator = fpq.iterator();
            int counter= 1;
            while(iterator.hasNext()){
                ShareVolume shareVolume = iterator.next();
                if(shareVolume!=null){
                    stringBuilder.append(counter++).append(")").append(shareVolume.getSymbol())
                            .append(":").append(numberFormat.format(shareVolume.getShares()))
                            .append(" ");
                }
            }
            return stringBuilder.toString();
        };



        //收到了reduce操作的影响 流切换成了表
        KTable<String, ShareVolume> shareVolume = builder.stream(STOCK_TRANSACTIONS_TOPIC,
                Consumed.with(stringSerde, stockTransactionSerde).withOffsetResetPolicy(EARLIEST))
                .mapValues(st -> ShareVolume.newBuilder(st).build())
                .groupBy((k, v) -> v.getSymbol(), Serialized.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);


        shareVolume.groupBy((k,v)-> KeyValue.pair(v.getIndustry(),v),Serialized.with(stringSerde,shareVolumeSerde))
                .aggregate(()->fixedQueue,
                        (k,v,agg)->agg.add(v),
                        (k,v,agg)->agg.remove(v),
                        Materialized.with(stringSerde,fixedSizePriorityQueueSerde))
                .mapValues(valueMapper)
                .toStream()
                .peek((k,v)->LOG.info("stock volume by industry {} {}",k,v))
                .to("stock-volume-by-company",Produced.with(stringSerde,stringSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        MockDataProducer.produceStockTransactions(15,50,25,false);
        kafkaStreams.start();

    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KTable-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KTable-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }
}
