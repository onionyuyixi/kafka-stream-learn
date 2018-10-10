package com.onion.kafkastreamlrean.chapter4;

import com.onion.kafkastreamlrean.clients.MockDataProducer;
import com.onion.kafkastreamlrean.model.Purchase;
import com.onion.kafkastreamlrean.model.PurchasePattern;
import com.onion.kafkastreamlrean.model.RewardAccumulator;
import com.onion.kafkastreamlrean.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMartKafkaStreamsAddStateApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsAddStateApp.class);

    public static void main(String[] args) {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        StreamsBuilder builder = new StreamsBuilder();

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();


        KStream<String, Purchase> transactions = builder.stream("transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = transactions.mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns----------"));

        patternKStream.to("patterns",Produced.with(stringSerde,purchasePatternSerde));

        //添加store 与 store 的 processor

        String rewardsStateStore = "rewardsStateStore"; //会生成APPLICATION_ID_CONFIG-storeName-changelog的主题
        RewardsStreamPartitioner rewardsStreamPartitioner = new RewardsStreamPartitioner(); // store的自定义分区

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStore); //获取一个store的提供员
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());//根据store的提供员 以及 序列化参数

        //向流中添加storeBulider
        builder.addStateStore(storeBuilder);

        KStream<String, Purchase> customer_transactions = transactions.through("customer_transactions", Produced.with(stringSerde, purchaseSerde, rewardsStreamPartitioner));

        KStream<String, RewardAccumulator> statefulRewardAccumutor = customer_transactions.transformValues(() -> new PurchaseRewardTransformer(rewardsStateStore), rewardsStateStore);
        statefulRewardAccumutor.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("store-rewards-*******"));

        statefulRewardAccumutor.to("rewards",Produced.with(stringSerde,rewardAccumulatorSerde));

        MockDataProducer.producePurchaseData();

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);

        kafkaStreams.start();


    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "AddingStateConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "AddingStateGroupId");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AddingStateAppId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }


}
