package org.example.kafkalabs.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String LONDON_MARATHON_INPUT_TOPIC = "london-marathon1.public.london_marathon";
    public static final String WINNERS_INPUT_TOPIC = "winners.public.winners";
    public static final String BRITISH_TOPIC = "british";
    public static final String MARATHONS_TILL_1990 = "marathons-till-1990";
    public static final String MARATHONS_FROM_1990_TO_2000 = "marathons-from-1990-to-2000";
    public static final String MARATHONS_FROM_2000 = "marathons-from-2000";
    public static final String AMOUNT_OF_BRITISH_WINNERS = "amount-of-british-winners";
    public static final String AMOUNT_OF_WINNERS_FROM_1990_TO_2000 = "amount-of-winners-from-1990-to-2000";
    public static final String JOINED_MARATHONS = "joined-marathons";
    public static final String WINDOWED_TOPIC = "windowed";

    public static final String PRODUCER_METRICS_TOPIC = "producer-metrics";

    @Bean
    @Qualifier("london-marathon1.public.london_marathon")
    public NewTopic londonMarathonTopic() {
        return TopicBuilder.name(LONDON_MARATHON_INPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("winners.public.winners")
    public NewTopic winnersTopic() {
        return TopicBuilder.name(WINNERS_INPUT_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("british")
    public NewTopic britishTopic() {
        return TopicBuilder.name(BRITISH_TOPIC)
                .build();
    }

    @Bean
    @Qualifier("marathons-till-1990")
    public NewTopic marathonsTill1900() {
        return TopicBuilder.name(MARATHONS_TILL_1990)
                .build();
    }

    @Bean
    @Qualifier("marathons-from-1990-to-2000")
    public NewTopic marathonsFrom1990Till2000() {
        return TopicBuilder.name(MARATHONS_FROM_1990_TO_2000)
                .build();
    }

    @Bean
    @Qualifier("marathons-from-2000")
    public NewTopic marathonsFrom2000() {
        return TopicBuilder.name(MARATHONS_FROM_2000)
                .build();
    }

    @Bean
    @Qualifier("amount-of-british-winners")
    public NewTopic amountOfBritishWinners() {
        return TopicBuilder.name(AMOUNT_OF_BRITISH_WINNERS)
                .build();
    }

    @Bean
    @Qualifier("amount-of-winners-from-1990-to-2000")
    public NewTopic amountOfWinnersFrom1900To2000() {
        return TopicBuilder.name(AMOUNT_OF_WINNERS_FROM_1990_TO_2000)
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
