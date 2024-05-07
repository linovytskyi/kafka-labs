package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafkalabs.model.MilkCowFact;
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

  //  @Autowired
    public void lab4Pipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream(MILK_COW_FACTS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));

        // 1. Порахувати кількість записів, де ціна за корову менше 1100.
        KTable<String, Long> cowsPriceLower1100 = messageStream.selectKey((key, value) -> KEY)
                .mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, MilkCowFact.class))
                .filter((key, value) -> value.getMilkCowCostPerAnimal() < 1100)
                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                .groupBy((key, value) -> key)
                .count();

        cowsPriceLower1100.toStream()
                .mapValues(Object::toString)
                .to(AMOUNT_WHERE_COW_PRICE_LOWER_1100);

        // 2. Порахувати скільки було вироблено молока за ті роки, де середня ціна за молоко була менше 0.13.
        KTable<String, Double> amountOfMilkProducedDuringYearsAvgPriceMilkLower013 =
                messageStream.selectKey((key, value) -> KEY)
                        .mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, MilkCowFact.class))
                        .filter((key, value) -> value.getAvgPriceMilk() < 0.13)
                        .mapValues(MilkCowFact::getMilkProductionLbs)
                        .groupBy((key, value) -> key, Grouped.with(Serdes.String(), Serdes.Double()))
                        .reduce(Double::sum);

        amountOfMilkProducedDuringYearsAvgPriceMilkLower013.toStream()
                .mapValues(value -> value * 0.44)
                .mapValues(value -> value.toString() + " liters")
                .to(AMOUNT_MILK_PRODUCED_DURING_YEARS_AVG_MILK_PRICE_LOWER_013, Produced.with(Serdes.String(), Serdes.String()));


        // 3. Зчитати у Kafka Stream потоки з п.2 лабораторної роботи №3 (результат розгалудження).
        // Об’єднати ці потоки за допомогою операцій join.
        KStream<String, String> lessThan013 = streamsBuilder
                .stream(LESS_THAN_013_OUTPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> moreEqualThan013LessEqualThan016 = streamsBuilder
                .stream(MORE_EQUAL_THAN_013_LESS_EQUAL_THAN_016_OUTPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> moreThan016 = streamsBuilder
                .stream(MORE_THAN_016_OUTPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));


        KStream<String, String> joinedStream = lessThan013
                .join(
                        moreEqualThan013LessEqualThan016,
                        StringUtil::getRandom,
                        JoinWindows.of(Duration.ofMinutes(5))
                )
                .join(
                        moreThan016,
                        StringUtil::getRandom,
                        JoinWindows.of(Duration.ofMinutes(5))
                );

        joinedStream.to(JOINED_AVG_PRICE_MILK_TOPIC);
    }
}
