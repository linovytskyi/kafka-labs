package org.example.kafkalabs.controller;

import org.apache.kafka.streams.StreamsBuilder;
import org.example.kafkalabs.model.MilkCowFact;
import org.example.kafkalabs.service.KafkaService;
import org.example.kafkalabs.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.MILK_COW_FACTS_INPUT_TOPIC;

@RestController
@RequestMapping("milk-cow-fact")
public class MilkCowFactController {
    private final KafkaService kafkaService;

    private final JdbcTemplate jdbcTemplate;

    private final KafkaStreams kafkaStreams;

    private static final Logger LOGGER = LoggerFactory.getLogger(MilkCowFactController.class);

    public MilkCowFactController(KafkaService kafkaService,
                                 JdbcTemplate jdbcTemplate,
                                 KafkaStreams kafkaStreams) {
        this.kafkaService = kafkaService;
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaStreams = kafkaStreams;
    }

    @PostMapping("kafka/random")
    @ResponseStatus(HttpStatus.OK)
    public void sendRandomFactToKafka() {
        LOGGER.warn("Get request to send random milk cow fact to kafka");
        kafkaService.sendToKafka(MilkCowFact.createRandomFact(), MILK_COW_FACTS_INPUT_TOPIC);
    }

    @PostMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public void save(@RequestBody MilkCowFact fact) {
        LOGGER.warn("Trying to save fact {} in database", fact);
        try {
            String sql = "INSERT INTO public.milkcow_facts (year, avg_milk_cow_number, milk_per_cow, milk_production_lbs, avg_price_milk, dairy_ration, milk_feed_price_ratio, milk_cow_cost_per_animal, milk_volume_to_buy_cow_in_lbs, alfalfa_hay_price, slaughter_cow_price) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            jdbcTemplate.update(sql,
                    fact.getYear(),
                    fact.getAvgMilkCowNumber(),
                    fact.getMilkPerCow(),
                    fact.getMilkProductionLbs(),
                    fact.getAvgPriceMilk(),
                    fact.getDairyRation(),
                    fact.getMilkFeedPriceRatio(),
                    fact.getMilkCowCostPerAnimal(),
                    fact.getMilkVolumeToBuyCowInLbs(),
                    fact.getAlfalfaHayPrice(),
                    fact.getSlaughterCowPrice());

            LOGGER.warn("Successfully inserted object to database");
        } catch (Exception e) {
            LOGGER.error("Error occurred while saving fact to db. Details {}", e.getMessage());
        }
    }

    @GetMapping("/trigger")
    @ResponseStatus(HttpStatus.OK)
    public void trigger(@Autowired StreamsBuilder streamsBuilder) {
        kafkaStreams.lab4PipelineCowsPrice(streamsBuilder);
    }
}
