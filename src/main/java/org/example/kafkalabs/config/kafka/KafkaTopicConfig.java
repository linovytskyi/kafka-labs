package org.example.kafkalabs.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String MILK_PRODUCT_FACTS_INPUT_TOPIC = "milk-products-facts.public.milk_products_facts";
    public static final String MILK_COW_FACTS_INPUT_TOPIC = "milk-cow-facts.public.milkcow_facts";

    public static final String LESS_THAN_1000_OUTPUT_TOPIC = "less-than-1000";

    public static final String LESS_THAN_013_OUTPUT_TOPIC = "less-than-013";
    public static final String MORE_EQUAL_THAN_013_LESS_EQUAL_THAN_016_OUTPUT_TOPIC = "more-equal-than-013-less-equal-than-016";
    public static final String MORE_THAN_016_OUTPUT_TOPIC = "more-than-016";

    @Bean
    @Qualifier("milk-products-facts.public.milk_products_facts")
    public NewTopic milkProductFactsTopic() {
        return TopicBuilder.name(MILK_PRODUCT_FACTS_INPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("milk-cow-facts.public.milkcow_facts")
    public NewTopic milkCowFactsTopic() {
        return TopicBuilder.name(MILK_COW_FACTS_INPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("less-than-1000-topic")
    public NewTopic lessThan1000Topic() {
        return TopicBuilder.name(LESS_THAN_1000_OUTPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("less-than-013")
    public NewTopic lessThan013() {
        return TopicBuilder.name(LESS_THAN_013_OUTPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("more-equal-than-013-less-equal-than-016")
    public NewTopic moreEqualThan013LessEqual016() {
        return TopicBuilder.name(MORE_EQUAL_THAN_013_LESS_EQUAL_THAN_016_OUTPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("more-than-016")
    public NewTopic moreThan016() {
        return TopicBuilder.name(MORE_THAN_016_OUTPUT_TOPIC)
                .build();
    }
}
