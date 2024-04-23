package org.example.kafkalabs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.kafkalabs.model.MilkProductFacts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class MilkProductsFactsKafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private final static Logger LOGGER = LoggerFactory.getLogger(MilkProductsFactsKafkaService.class);

    public MilkProductsFactsKafkaService(KafkaTemplate<String, String> kafkaTemplate,
                                         ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendRandomObject(String topic) {
        MilkProductFacts randomObject = MilkProductFacts.createRandomObject();
        LOGGER.info("Created random object - {}", randomObject);
        sendToKafka(randomObject, topic);
    }

    public void sendToKafka(MilkProductFacts fact, String topic) {
        ObjectNode payloadNode = buildPayloadNodeForFact(fact);
        LOGGER.info("Trying to send {} to topic {}", payloadNode.toPrettyString(), topic);
        try {
            kafkaTemplate.send(topic, objectMapper.writeValueAsString(payloadNode));
            LOGGER.info("Successfully sent object to topic {}", topic);
        } catch (Exception e) {
            LOGGER.error("Error occurred while putting object {} to topic {}. Details {}", fact, topic, e.getMessage());
        }
    }

    private ObjectNode buildPayloadNodeForFact(MilkProductFacts fact) {
        ObjectNode rootNode = objectMapper.createObjectNode();
        ObjectNode valueNode = objectMapper.valueToTree(fact);
        rootNode.set("after", valueNode);

        ObjectNode payloadNode = objectMapper.createObjectNode();
        payloadNode.set("payload", rootNode);
        return payloadNode;
    }
}
