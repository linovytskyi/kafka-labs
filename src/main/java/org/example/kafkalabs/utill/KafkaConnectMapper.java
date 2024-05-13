package org.example.kafkalabs.utill;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class KafkaConnectMapper {

    private final ObjectMapper objectMapper;
    public KafkaConnectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public  <T> T getObjectFromStringMessage(String message, Class<T> classConvertMessageTo) {
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            JsonNode payloadNode = jsonNode.get("payload").get("after");
            return objectMapper.readValue(payloadNode.toPrettyString(), classConvertMessageTo);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON message: " + e.getMessage());
        }
    }

    public <T> String mapObjectToStringMessage(T object) {
        ObjectNode rootNode = objectMapper.createObjectNode();
        ObjectNode valueNode = objectMapper.valueToTree(object);
        rootNode.set("after", valueNode);
        ObjectNode payloadNode = objectMapper.createObjectNode();
        payloadNode.set("payload", rootNode);
        System.out.println(payloadNode.toPrettyString());
        try {
            return objectMapper.writeValueAsString(payloadNode);
        } catch (Exception e) {
            throw new RuntimeException("Error parsing object to JSON. " + e.getMessage());
        }
    }
}
