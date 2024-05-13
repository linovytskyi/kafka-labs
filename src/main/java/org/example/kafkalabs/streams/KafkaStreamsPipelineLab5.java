/*
package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafkalabs.model.MilkCowFact;
import org.example.kafkalabs.utill.KafkaConnectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.*;

@Component
public class KafkaStreamsPipelineLab5 {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private final KafkaConnectMapper kafkaConnectMapper;

    private static final String KEY = "KEY";

    public KafkaStreamsPipelineLab5(KafkaConnectMapper kafkaConnectMapper) {
        this.kafkaConnectMapper = kafkaConnectMapper;
    }

    @Autowired
    public void lab5Pipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream(MILK_COW_FACTS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));

        KGroupedStream<String, String> cowsPriceLower1100Grouped = messageStream.selectKey((key, value) -> KEY)
                .mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, MilkCowFact.class))
                .filter((key, value) -> value.getMilkCowCostPerAnimal() < 1100)
                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                .groupByKey();

        // Вікно фіксованого розміру, що не перекривається
        TimeWindows tumblingWindows = TimeWindows.of(Duration.ofSeconds(20));
        KTable<Windowed<String>, Long>  tumbled = cowsPriceLower1100Grouped
                .windowedBy(tumblingWindows)
                .count();

        // Вікно фіксованого розміру, що перекривається
        */
/*TimeWindows hoppingWindow = TimeWindows.of(Duration.ofSeconds(10))
                .advanceBy(Duration.ofSeconds(2));
        KTable<Windowed<String>, Long>  hopping = cowsPriceLower1100Grouped
                .windowedBy(hoppingWindow)
                .count();*//*


        // Вікно cесії
       */
/* Duration inactivityDuration = Duration.ofSeconds(10);
        KTable<Windowed<String>, Long>  session = cowsPriceLower1100Grouped
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityDuration))
                .count();*//*


        tumbled.toStream()
                .selectKey((key, value) -> KEY)
                .mapValues(Object::toString)
                .to(WINDOWED_TOPIC);
    }
}
*/
