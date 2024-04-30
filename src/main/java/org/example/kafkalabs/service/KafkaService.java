package org.example.kafkalabs.service;

import org.example.kafkalabs.model.MilkProductFact;
import org.example.kafkalabs.utill.KafkaConnectMapperUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConnectMapperUtil kafkaConnectMapperUtil;

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaService.class);

    public KafkaService(KafkaTemplate<String, String> kafkaTemplate,
                                         KafkaConnectMapperUtil kafkaConnectMapperUtil) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaConnectMapperUtil = kafkaConnectMapperUtil;
    }

    public <T> void sendToKafka(T object, String topic) {
        String messageToSend = kafkaConnectMapperUtil.mapObjectToStringMessage(object);
        LOGGER.warn("Trying to send {} to topic {}", messageToSend, topic);
        try {
            kafkaTemplate.send(topic, messageToSend);
            LOGGER.warn("Successfully sent object to topic {}", topic);
        } catch (Exception e) {
            LOGGER.error("Error occurred while putting object {} to topic {}. Details {}", object, topic, e.getMessage());
        }
    }
}
