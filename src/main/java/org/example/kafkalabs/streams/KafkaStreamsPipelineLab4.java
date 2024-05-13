package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafkalabs.model.Winner;
import org.example.kafkalabs.utill.KafkaConnectMapper;
import org.example.kafkalabs.utill.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.*;

@Component
public class KafkaStreamsPipelineLab4 {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private final KafkaConnectMapper kafkaConnectMapper;

    private static final String KEY = "KEY";

    public KafkaStreamsPipelineLab4(KafkaConnectMapper kafkaConnectMapper) {
        this.kafkaConnectMapper = kafkaConnectMapper;
    }

    @Autowired
    public void lab4Pipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream(WINNERS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));

        // 1. Порахувати кількість бігунів Великої Британії.
        String britishNat = "United Kingdom";
        KTable<String, Long> amountOfBritishWinners = messageStream.selectKey((key, value) -> KEY)
                .mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, Winner.class))
                .filter((key, value) -> value.getNationality().equals(britishNat))
                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                .groupBy((key, value) -> key)
                .count();

        amountOfBritishWinners.toStream()
                .mapValues(Object::toString)
                .to(AMOUNT_OF_BRITISH_WINNERS);

        //2. Порахувати кількість людей, що добігли до фінішу у період з 1990 до 2000 років.
        KTable<String, Long> amountOfWinnersFrom1900To2000 =
                messageStream.selectKey((key, value) -> KEY)
                        .mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, Winner.class))
                        .filter((key, value) -> value.getYear() >= 1990 && value.getYear() <= 2000)
                        .groupByKey()
                        .count();

        amountOfWinnersFrom1900To2000.toStream()
                .to(AMOUNT_OF_WINNERS_FROM_1990_TO_2000);


        // 3. Зчитати у Kafka Stream потоки з п.2 лабораторної роботи №3 (результат розгалудження).
        // Об’єднати ці потоки за допомогою операцій join.
        KStream<String, String> till1990 = streamsBuilder
                .stream(MARATHONS_TILL_1990, Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> from1990to2000 = streamsBuilder
                .stream(MARATHONS_FROM_1990_TO_2000, Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> from2000 = streamsBuilder
                .stream(MARATHONS_FROM_2000, Consumed.with(STRING_SERDE, STRING_SERDE));


        KStream<String, String> joinedStream = till1990
                .join(
                        from1990to2000,
                        StringUtil::getRandom,
                        JoinWindows.of(Duration.ofMinutes(5))
                )
                .join(
                        from2000,
                        StringUtil::getRandom,
                        JoinWindows.of(Duration.ofMinutes(5))
                );

        joinedStream.to(JOINED_MARATHONS);
    }
}
