package org.example.kafkalabs.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String LONDON_MARATHON_INPUT_TOPIC = "london-marathon.public.london_marathon";
    public static final String WINDOWED_TOPIC = "windowed";

    public static final String PRODUCER_METRICS_TOPIC = "producer-metrics";

    @Bean
    @Qualifier("milk-products-facts.public.milk_products_facts")
    public NewTopic milkProductFactsTopic() {
        return TopicBuilder.name(LONDON_MARATHON_INPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("windowed")
    public NewTopic windowedTopic() {
        return TopicBuilder.name(WINDOWED_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("producer-metrics")
    public NewTopic producerMetrics() {
        return TopicBuilder.name(PRODUCER_METRICS_TOPIC)
                .build();
    }
}
