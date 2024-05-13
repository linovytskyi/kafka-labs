package org.example.kafkalabs.controller;

import org.example.kafkalabs.model.LondonMarathon;
import org.example.kafkalabs.model.Winner;
import org.example.kafkalabs.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.LONDON_MARATHON_INPUT_TOPIC;

@RestController
@RequestMapping("winners")
public class WinnersController {
    private final KafkaService kafkaService;

    private final JdbcTemplate jdbcTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(LondonMarathonController.class);

    public WinnersController(KafkaService kafkaService,
                                    JdbcTemplate jdbcTemplate) {
        this.kafkaService = kafkaService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping("kafka/random")
    @ResponseStatus(HttpStatus.OK)
    public void sendRandomFactToKafka() {
        LOGGER.warn("Get request to send random winner to kafka");
        kafkaService.sendToKafka(LondonMarathon.createRandomEvent(), LONDON_MARATHON_INPUT_TOPIC);
    }

    @PostMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public void save(@RequestBody Winner winner) {
        LOGGER.warn("Trying to save winner {} in database", winner);
        try {
            String sql = "INSERT INTO public.winners (category, year, athlete, nationality, time) " +
                    "VALUES (?, ?, ?, ?, ?)";

            jdbcTemplate.update(sql,
                    winners.getCategory(),
                    winners.getYear(),
                    winners.getAthlete(),
                    winners.getNationality(),
                    winners.getTime());

            LOGGER.warn("Successfully inserted object to database");
        } catch (Exception e) {
            LOGGER.error("Error occurred while saving fact to db. Details {}", e.getMessage());
        }
    }
}
