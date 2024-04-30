package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafkalabs.model.MilkCowFact;
import org.example.kafkalabs.utill.KafkaConnectMapperUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.*;

@Component
public class KafkaStreams {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private final KafkaConnectMapperUtil kafkaConnectMapperUtil;

    private final KStream<String, String> messageStream;

    public KafkaStreams(KafkaConnectMapperUtil kafkaConnectMapperUtil,
                        StreamsBuilder streamsBuilder) {
        this.kafkaConnectMapperUtil = kafkaConnectMapperUtil;
        messageStream = streamsBuilder
                .stream(MILK_COW_FACTS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));
    }

    /*@Autowired
    void lab3Pipeline(StreamsBuilder streamsBuilder) {

        KStream<String, String> messageStream = streamsBuilder
                .stream(MILK_COW_FACTS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));


        // 1. Відфільтрувати записи, де ціна за корову менше 1100.
        messageStream.mapValues(value -> kafkaConnectMapperUtil.getObjectFromStringMessage(value, MilkCowFact.class))
                .filter((key, value) -> value.getMilkCowCostPerAnimal() < 1100)
                .mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                .to(LESS_THAN_1000_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 2. Розділити записи на три гілки: середня ціна за молоко менше 0.13, від 0.13 до 0.16, більше 0.16.
        // Записати результати у різні тем
        messageStream.mapValues(value -> kafkaConnectMapperUtil.getObjectFromStringMessage(value, MilkCowFact.class))
                .split()
                .branch((key, value) -> value.getAvgPriceMilk() < 0.13, Branched.withConsumer(
                        (ks) -> ks.mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                        .to(LESS_THAN_013_OUTPUT_TOPIC)))
                .branch((key, value) -> value.getAvgPriceMilk() >= 0.13 && value.getAvgPriceMilk() <= 0.16, Branched.withConsumer(
                        (ks) -> ks.mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                        .to(MORE_EQUAL_THAN_013_LESS_EQUAL_THAN_016_OUTPUT_TOPIC)))
                .branch((key, value) -> value.getAvgPriceMilk() > 0.16, Branched.withConsumer(
                        (ks) -> ks.mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                        .to(MORE_THAN_016_OUTPUT_TOPIC)))
                .defaultBranch();
    }*/


    @Autowired
    public void lab4PipelineCowsPrice(StreamsBuilder streamsBuilder) {
        System.out.println("triggered");
        KStream<String, String> messageStream = streamsBuilder
                .stream(MILK_COW_FACTS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));
       KTable<String, Long> cowsPriceLower1100 = messageStream.selectKey((key, value) -> "Amount of cows price lower 1000: ")
                .mapValues(value -> kafkaConnectMapperUtil.getObjectFromStringMessage(value, MilkCowFact.class))
                .filter((key, value) -> value.getMilkCowCostPerAnimal() < 1100)
                .mapValues(kafkaConnectMapperUtil::mapObjectToStringMessage)
                .groupBy((key, value) -> key)
                .count();

        cowsPriceLower1100.toStream().foreach((key, count) -> System.out.println(key + " " + count));
    }

    public void lab4PipelineAmountOfMilk() {
        KTable<String, Double> amountOfMilkProducedDuringYearsAvgPriceMilkLower013 =
                messageStream.mapValues(value -> kafkaConnectMapperUtil.getObjectFromStringMessage(value, MilkCowFact.class))
                        .filter((key, value) -> value.getAvgPriceMilk() < 0.13)
                        .map((key, value) -> KeyValue.pair("Total amount of milk produced in lbs during years where avg price milk lower 0.13", value.getMilkProductionLbs()))
                        .groupBy((key, value) -> key)
                        .reduce(Double::sum);

        amountOfMilkProducedDuringYearsAvgPriceMilkLower013.toStream().foreach((key, count) -> System.out.println(key + " " + (count * 0.44) + " liters"));
    }
}
