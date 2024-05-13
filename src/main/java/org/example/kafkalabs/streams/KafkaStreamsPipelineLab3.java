package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafkalabs.model.LondonMarathon;
import org.example.kafkalabs.model.Winner;
import org.example.kafkalabs.utill.KafkaConnectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
        KStream<String, String> winnersStream = streamsBuilder
                .stream(WINNERS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));


        // 1. Відфільтрувати записи бігунів Великої Британії.
        String britishNat = "United Kingdom";
        winnersStream.mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, Winner.class))
                .filter((key, value) -> value.getNationality().equals(britishNat))
                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                .to(BRITISH_TOPIC, Produced.with(Serdes.String(), Serdes.String()));


        // 2. Розділити записи на три гілки: рік марафону до 1990, від 1990 до 2000, після 2000. Записати результати у різні теми
        winnersStream.mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, Winner.class))
                .split()
                .branch((key, value) -> value.getYear() < 1990, Branched.withConsumer(
                        (ks) -> ks.selectKey((key, value) -> KEY)
                                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                                .to(MARATHONS_TILL_1990)))
                .branch((key, value) -> value.getYear() >= 1990 && value.getYear() <= 2000, Branched.withConsumer(
                        (ks) -> ks.selectKey((key, value) -> KEY)
                                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                                .to(MARATHONS_FROM_1990_TO_2000)))
                .branch((key, value) -> value.getYear() > 2000, Branched.withConsumer(
                        (ks) -> ks.selectKey((key, value) -> KEY)
                                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                                .to(MARATHONS_FROM_2000)))
                .defaultBranch();
    }
}
