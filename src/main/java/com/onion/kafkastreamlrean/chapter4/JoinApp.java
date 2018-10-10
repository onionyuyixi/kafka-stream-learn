package com.onion.kafkastreamlrean.chapter4;

import com.onion.kafkastreamlrean.clients.MockDataProducer;
import com.onion.kafkastreamlrean.model.CorrelatedPurchase;
import com.onion.kafkastreamlrean.model.Purchase;
import com.onion.kafkastreamlrean.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class JoinApp {

    private static final Logger LOG = LoggerFactory.getLogger(JoinApp.class);

    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        StreamsBuilder builder = new StreamsBuilder();

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        KeyValueMapper<String,Purchase,KeyValue<String,Purchase>> custIdCCMasking = (k,v)-> {
            Purchase purchase = Purchase.builder(v).maskCreditCard().build();
            return new KeyValue<>(purchase.getCustomerId(),purchase);
        };

        Predicate<String,Purchase> coffeePredicate = (key,purchase)->purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String,Purchase> eletronicsPredicate = (key,purchase)->purchase.getDepartment().equalsIgnoreCase("eletronics");

        final int COFFEE_PURCHASE = 0;
        final int ELECTRONICS_PURCHASE = 1;

        KStream<String, Purchase> onion_transactions = builder.stream("onion_transactions", Consumed.with(stringSerde, purchaseSerde)).map(custIdCCMasking);


        KStream<String, Purchase>[] branch = onion_transactions.branch(coffeePredicate, eletronicsPredicate);
        KStream<String, Purchase> coffeeStream = branch[COFFEE_PURCHASE];
        KStream<String, Purchase> electronicsStream = branch[ELECTRONICS_PURCHASE];

        PurchaseJoiner purchaseJoiner = new PurchaseJoiner();
        JoinWindows minuteWindow = JoinWindows.of(60 * 1000 * 20);


        KStream<String, CorrelatedPurchase> join = coffeeStream.join(electronicsStream,
                purchaseJoiner, minuteWindow, Joined.with(stringSerde, purchaseSerde, purchaseSerde));


        join.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("join**********"));

        MockDataProducer.producePurchaseData();

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);


        kafkaStreams.start();







    }





    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TranscationTimestampExtractor.class);
        return props;
    }
}
