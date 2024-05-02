package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafkalabs.model.MilkCowFact;
import org.example.kafkalabs.utill.KafkaConnectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Random;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.*;

@Component
public class KafkaStreamsPipelineLab3 {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private final KafkaConnectMapper kafkaConnectMapper;

    private static final String KEY = "KEY";

    public KafkaStreamsPipelineLab3(KafkaConnectMapper kafkaConnectMapper) {
        this.kafkaConnectMapper = kafkaConnectMapper;
    }

    @Autowired
    void lab3Pipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream(MILK_COW_FACTS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));


        // 1. Відфільтрувати записи, де ціна за корову менше 1100.
        /*messageStream.mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, MilkCowFact.class))
                .filter((key, value) -> value.getMilkCowCostPerAnimal() < 1100)
                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                .to(LESS_THAN_1000_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));*/

        // 2. Розділити записи на три гілки: середня ціна за молоко менше 0.13, від 0.13 до 0.16, більше 0.16. Записати результати у різні тем
        messageStream.mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, MilkCowFact.class))
                .split()
                .branch((key, value) -> value.getAvgPriceMilk() < 0.13, Branched.withConsumer(
                        (ks) -> ks.selectKey((key, value) -> KEY)
                                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                        .to(LESS_THAN_013_OUTPUT_TOPIC)))
                .branch((key, value) -> value.getAvgPriceMilk() >= 0.13 && value.getAvgPriceMilk() <= 0.16, Branched.withConsumer(
                        (ks) -> ks.selectKey((key, value) -> KEY)
                                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                        .to(MORE_EQUAL_THAN_013_LESS_EQUAL_THAN_016_OUTPUT_TOPIC)))
                .branch((key, value) -> value.getAvgPriceMilk() > 0.16, Branched.withConsumer(
                        (ks) -> ks.selectKey((key, value) -> KEY)
                                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                        .to(MORE_THAN_016_OUTPUT_TOPIC)))
                .defaultBranch();
    }
}
