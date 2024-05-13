package org.example.kafkalabs.controller;

import org.example.kafkalabs.model.LondonMarathon;
import org.example.kafkalabs.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.LONDON_MARATHON_INPUT_TOPIC;

@RestController
@RequestMapping("london-marathon")
public class LondonMarathonController {
    private final KafkaService kafkaService;

    private final JdbcTemplate jdbcTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(LondonMarathonController.class);

    public LondonMarathonController(KafkaService kafkaService,
                                      JdbcTemplate jdbcTemplate) {
        this.kafkaService = kafkaService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping("kafka/random")
    @ResponseStatus(HttpStatus.OK)
    public void sendRandomFactToKafka() {
        LOGGER.warn("Get request to send random milk product fact to kafka");
        kafkaService.sendToKafka(LondonMarathon.createRandomEvent(), LONDON_MARATHON_INPUT_TOPIC);
    }

    @PostMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public void save(@RequestBody LondonMarathon londonMarathon) {
        LOGGER.warn("Trying to save fact {} in database", londonMarathon);
        try {
            String sql = "INSERT INTO public.london_marathon (date, year, applicants, accepted, starters, finishers, raised, \"Official charity\") " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            jdbcTemplate.update(sql,
                    londonMarathon.getDate(),
                    londonMarathon.getYear(),
                    londonMarathon.getApplicants(),
                    londonMarathon.getAccepted(),
                    londonMarathon.getStarters(),
                    londonMarathon.getFinishers(),
                    londonMarathon.getRaised(),
                    londonMarathon.getOfficialCharity());

            LOGGER.warn("Successfully inserted object to database");
        } catch (Exception e) {
            LOGGER.error("Error occurred while saving fact to db. Details {}", e.getMessage());
        }
    }
}
