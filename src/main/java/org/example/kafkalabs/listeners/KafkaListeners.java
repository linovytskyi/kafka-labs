package org.example.kafkalabs.listeners;

import org.example.kafkalabs.model.MilkCowFact;
import org.example.kafkalabs.model.MilkProductFact;
import org.example.kafkalabs.utill.KafkaConnectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.*;

@Component
public class KafkaListeners {

    private final KafkaConnectMapper kafkaConnectMapper;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaListeners.class);

    public KafkaListeners(KafkaConnectMapper kafkaConnectMapper) {
        this.kafkaConnectMapper = kafkaConnectMapper;
    }

    @KafkaListener(topics = "milk-products-facts.public.milk_products_facts", groupId = "None", containerFactory = "listenerFactory")
    void listenMilkProductFacts(String message) {
        MilkProductFact fact = kafkaConnectMapper.getObjectFromStringMessage(message, MilkProductFact.class);
        logProcessMessage(fact, MILK_PRODUCT_FACTS_INPUT_TOPIC);
    }

    @KafkaListener(topics = "milk-cow-facts.public.milkcow_facts", groupId = "None", containerFactory = "listenerFactory")
    void listenMilkCowFact(String message) {
        MilkCowFact fact = kafkaConnectMapper.getObjectFromStringMessage(message, MilkCowFact.class);
        logProcessMessage(fact, MILK_COW_FACTS_INPUT_TOPIC);
    }

    @KafkaListener(topics = "less-than-1000", groupId = "None", containerFactory = "listenerFactory")
    void listenLessThan1000(String message) {
        MilkCowFact fact = kafkaConnectMapper.getObjectFromStringMessage(message, MilkCowFact.class);
        logProcessMessage(fact, LESS_THAN_1000_OUTPUT_TOPIC);
    }

    @KafkaListener(topics = "less-than-013", groupId = "None", containerFactory = "listenerFactory")
    void listenLessThan013(String message) {
        MilkCowFact fact = kafkaConnectMapper.getObjectFromStringMessage(message, MilkCowFact.class);
        logProcessMessage(fact, LESS_THAN_013_OUTPUT_TOPIC);
    }

    @KafkaListener(topics = "more-equal-than-013-less-equal-than-016", groupId = "None", containerFactory = "listenerFactory")
    void listenMoreEqualThan013LessEqualThan016(String message) {
        MilkCowFact fact = kafkaConnectMapper.getObjectFromStringMessage(message, MilkCowFact.class);
        logProcessMessage(fact, MORE_EQUAL_THAN_013_LESS_EQUAL_THAN_016_OUTPUT_TOPIC);
    }

    @KafkaListener(topics = "more-than-016", groupId = "None", containerFactory = "listenerFactory")
    void listenMoreThan016(String message) {
        MilkCowFact fact = kafkaConnectMapper.getObjectFromStringMessage(message, MilkCowFact.class);
        logProcessMessage(fact, MORE_THAN_016_OUTPUT_TOPIC);
    }

    @KafkaListener(topics = "amount-where-cow-price-lower-1100", groupId = "None", containerFactory = "listenerFactory")
    void listenAmountWhereCowPriceLower1100(String message) {
        logProcessMessage(message, AMOUNT_WHERE_COW_PRICE_LOWER_1100);
    }

    @KafkaListener(topics = "amount-milk-produced-during-years-avg-milk-price-lower-013", groupId = "None", containerFactory = "listenerFactory")
    void listenAmountMilkProducedDuringYearsAvgMilkPriceLower013(String message) {
        logProcessMessage(message, AMOUNT_MILK_PRODUCED_DURING_YEARS_AVG_MILK_PRICE_LOWER_013);
    }

    @KafkaListener(topics = "joined-avg-price-milk", groupId = "None", containerFactory = "listenerFactory")
    void listedJoinedAvgPrice(String message) {
        MilkCowFact milkCowFact = kafkaConnectMapper.getObjectFromStringMessage(message, MilkCowFact.class);
        logProcessMessage(milkCowFact, JOINED_AVG_PRICE_MILK_TOPIC);
    }

    private static <T> void logProcessMessage(T processedMessage, String topic) {
        LOGGER.warn("Topic - {} has new message.",topic);
        LOGGER.warn("Processed message object - {}", processedMessage);
    }
}
