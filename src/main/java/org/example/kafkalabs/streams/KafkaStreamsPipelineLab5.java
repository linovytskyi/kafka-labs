package org.example.kafkalabs.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafkalabs.model.Winner;
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

   // @Autowired
    public void lab5Pipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream(WINNERS_INPUT_TOPIC, Consumed.with(STRING_SERDE, STRING_SERDE));

        String britishNat = "United Kingdom";
        KGroupedStream<String, String> amountOfBritishWinners = messageStream.selectKey((key, value) -> KEY)
                .mapValues(value -> kafkaConnectMapper.getObjectFromStringMessage(value, Winner.class))
                .filter((key, value) -> value.getNationality().equals(britishNat))
                .mapValues(kafkaConnectMapper::mapObjectToStringMessage)
                .groupByKey();

        // Вікно фіксованого розміру, що не перекривається
        TimeWindows tumblingWindows = TimeWindows.of(Duration.ofSeconds(20));
        KTable<Windowed<String>, Long>  tumbled = amountOfBritishWinners
                .windowedBy(tumblingWindows)
                .count();

       /* // Вікно фіксованого розміру, що перекривається
TimeWindows hoppingWindow = TimeWindows.of(Duration.ofSeconds(10))
                .advanceBy(Duration.ofSeconds(2));
        KTable<Windowed<String>, Long>  hopping = amountOfBritishWinners
                .windowedBy(hoppingWindow)
                .count();


        // Вікно cесії
 Duration inactivityDuration = Duration.ofSeconds(10);
        KTable<Windowed<String>, Long>  session = amountOfBritishWinners
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityDuration))
                .count();
*/

        tumbled.toStream()
                .selectKey((key, value) -> KEY)
                .mapValues(Object::toString)
                .to(WINDOWED_TOPIC);
    }
}
