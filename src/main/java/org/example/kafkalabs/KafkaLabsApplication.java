package org.example.kafkalabs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.opencsv.CSVReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.kafkalabs.model.MilkProductFacts;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.FileReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class KafkaLabsApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaLabsApplication.class, args);
    }

    @Bean
    CommandLineRunner commandLineRunner(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            for (int i = 0; i < 2; i++) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
                ObjectNode rootNode = objectMapper.createObjectNode();
                ObjectNode valueNode = objectMapper.valueToTree(MilkProductFacts.createRandomObject());
                rootNode.set("after", valueNode);

                ObjectNode payloadNode = objectMapper.createObjectNode();
                payloadNode.set("payload", rootNode);

                kafkaTemplate.send("milk-products-facts.public.milk_products_facts", objectMapper.writeValueAsString(payloadNode));
            }
        };
    }
}
