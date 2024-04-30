package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.example.kafkalabs.model.MilkCowFact;
import org.example.kafkalabs.utill.KafkaConnectMapperUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.*;

@Component
public class KafkaStreams {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private final KafkaConnectMapperUtil kafkaConnectMapperUtil;

    private static final String INPUT_TOPIC = "milk-cow-facts.public.milkcow_facts";

    public KafkaStreams(KafkaConnectMapperUtil kafkaConnectMapperUtil) {
        this.kafkaConnectMapperUtil = kafkaConnectMapperUtil;
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {

        KStream<String, String> messageStream = streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));


        // 1. Відфільтрувати записи, де ціна за корову менше 1100.
        messageStream.mapValues(value -> kafkaConnectMapperUtil.getObjectFromStringMessage(value, MilkCowFact.class))
                .filter((key, value) -> value.getMilkCowCostPerAnimal() < 1100)
                .mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                .to(LESS_THAN_1000_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 2. Розділити записи на три гілки: середня ціна за молоко менше 0.13, від 0.13 до 0.16, більше 0.16. Записати результати у різні тем
        messageStream.mapValues(value -> kafkaConnectMapperUtil.getObjectFromStringMessage(value, MilkCowFact.class))
                .split()
                .branch((key, value) -> value.getAvgPriceMilk() < 0.13, Branched.withConsumer((ks) -> ks.mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                        .to(LESS_THAN_013_OUTPUT_TOPIC)))
                .branch((key, value) -> value.getAvgPriceMilk() >= 0.13 && value.getAvgPriceMilk() <= 0.16, Branched.withConsumer((ks) -> ks.mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                        .to(MORE_EQUAL_THAN_013_LESS_EQUAL_THAN_016_OUTPUT_TOPIC)))
                .branch((key, value) -> value.getAvgPriceMilk() > 0.16, Branched.withConsumer((ks) -> ks.mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                        .to(MORE_THAN_016_OUTPUT_TOPIC)))
                .defaultBranch();
    }
}
