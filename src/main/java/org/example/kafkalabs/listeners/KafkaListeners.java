package org.example.kafkalabs.listeners;

import org.example.kafkalabs.model.LondonMarathon;
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
        LondonMarathon fact = kafkaConnectMapper.getObjectFromStringMessage(message, LondonMarathon.class);
        logProcessMessage(fact, LONDON_MARATHON_INPUT_TOPIC);
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
