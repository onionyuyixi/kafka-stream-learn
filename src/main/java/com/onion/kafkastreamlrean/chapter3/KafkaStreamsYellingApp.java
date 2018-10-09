package com.onion.kafkastreamlrean.chapter3;

import com.onion.kafkastreamlrean.clients.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.tomcat.util.http.fileupload.util.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamsYellingApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsYellingApp.class);

    public static void main(String[] args) {
        MockDataProducer.produceRandomTextData();

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"yelling_app_id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        StreamsConfig streamsConfig = new StreamsConfig(properties);
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("src-topic", Consumed.with(stringSerde, stringSerde));
        KStream<String, String> upperCasedStream = stream.mapValues(String::toUpperCase);
        upperCasedStream.to("out-topic",Produced.with(stringSerde,stringSerde));
        upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("Yelling App"));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.start();


    }
}
