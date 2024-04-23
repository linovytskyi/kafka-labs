package org.example.kafkalabs.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.example.kafkalabs.model.MilkProductFacts;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {

    @KafkaListener(topics = "milk-products-facts.public.milk_products_facts", groupId = "None", containerFactory = "listenerFactory")
    void listen(String message) {
        System.out.println("Kafka Listener received message:");
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            JsonNode payloadNode = jsonNode.get("payload").get("after");
            MilkProductFacts milkProductFacts = objectMapper.readValue(payloadNode.toPrettyString(), MilkProductFacts.class);
            System.out.println("Received payload: " + payloadNode.toPrettyString());
            System.out.println("Java object " + milkProductFacts);
        } catch (Exception e) {
            System.out.println("Error parsing JSON message: " + e.getMessage());
        }
    }
}
