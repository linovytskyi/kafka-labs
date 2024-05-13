package org.example.kafkalabs.listeners;

import org.example.kafkalabs.model.LondonMarathon;
import org.example.kafkalabs.model.Winner;
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

    @KafkaListener(topics = "london-marathon1.public.london_marathon", groupId = "None")
    void listenLondonMarathon(String message) {
        LondonMarathon marathon = kafkaConnectMapper.getObjectFromStringMessage(message, LondonMarathon.class);
        logProcessMessage(marathon, LONDON_MARATHON_INPUT_TOPIC);
    }

    @KafkaListener(topics = "winners.public.winners", groupId = "None")
    void listenWinners(String message) {
        Winner winner = kafkaConnectMapper.getObjectFromStringMessage(message, Winner.class);
        logProcessMessage(winner, WINNERS_INPUT_TOPIC);
    }

    @KafkaListener(topics = "british", groupId = "None")
    void listenBritish(String message) {
        Winner winner = kafkaConnectMapper.getObjectFromStringMessage(message, Winner.class);
        logProcessMessage(winner, BRITISH_TOPIC);
    }

    @KafkaListener(topics = MARATHONS_TILL_1990, groupId = "None")
    void listenMarathonsTill1990(String message) {
        Winner winner = kafkaConnectMapper.getObjectFromStringMessage(message, Winner.class);
        logProcessMessage(winner, MARATHONS_TILL_1990);
    }

    @KafkaListener(topics = MARATHONS_FROM_1990_TO_2000, groupId = "None")
    void listenMarathonsFrom1990To2000(String message) {
        Winner winner = kafkaConnectMapper.getObjectFromStringMessage(message, Winner.class);
        logProcessMessage(winner, MARATHONS_FROM_1990_TO_2000);
    }

    @KafkaListener(topics = MARATHONS_FROM_2000, groupId = "None")
    void listenMarathonsFrom2000(String message) {
        Winner winner = kafkaConnectMapper.getObjectFromStringMessage(message, Winner.class);
        logProcessMessage(winner, MARATHONS_FROM_2000);
    }

    @KafkaListener(topics = AMOUNT_OF_BRITISH_WINNERS, groupId = "None")
    void listenAmountOfBritishWinners(String message) {
        logProcessMessage(message, AMOUNT_OF_BRITISH_WINNERS);
    }

    @KafkaListener(topics = AMOUNT_OF_WINNERS_FROM_1990_TO_2000, groupId = "None")
    void listenAmountOfWinnersFrom1990To2000String(String message) {
        logProcessMessage(message, MARATHONS_FROM_2000);
    }

    @KafkaListener(topics = JOINED_MARATHONS, groupId = "None")
    void joinedWinners(String message) {
        Winner winner = kafkaConnectMapper.getObjectFromStringMessage(message, Winner.class);
        logProcessMessage(winner, JOINED_MARATHONS);
    }


    @KafkaListener(topics = "windowed", groupId = "None", containerFactory = "listenerFactory")
    void listenWindowedTopic(String message) {
        logProcessMessage(message, WINDOWED_TOPIC);
    }

    @KafkaListener(topics = "producer-metrics", groupId = "None", containerFactory = "listenerFactory")
    void listenProducerMetricsTopic(String message) {
        logProcessMessage(message, PRODUCER_METRICS_TOPIC);
    }

    private static <T> void logProcessMessage(T processedMessage, String topic) {
        LOGGER.warn("Topic - {} has new message.",topic);
        LOGGER.warn("Processed message object - {}", processedMessage);
    }
}
