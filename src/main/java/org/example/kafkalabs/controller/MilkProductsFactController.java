package org.example.kafkalabs.controller;

import org.example.kafkalabs.model.MilkProductFact;
import org.example.kafkalabs.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import static org.example.kafkalabs.config.kafka.KafkaTopicConfig.MILK_PRODUCT_FACTS_INPUT_TOPIC;

@RestController()
@RequestMapping("milk-products-fact")
public class MilkProductsFactController {

    private final KafkaService kafkaService;

    private final JdbcTemplate jdbcTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(MilkProductsFactController.class);

    public MilkProductsFactController(KafkaService kafkaService,
                                      JdbcTemplate jdbcTemplate) {
        this.kafkaService = kafkaService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping("kafka/random")
    @ResponseStatus(HttpStatus.OK)
    public void sendRandomFactToKafka() {
        LOGGER.warn("Get request to send random milk product fact to kafka");
        kafkaService.sendToKafka(MilkProductFact.createRandomFact(), MILK_PRODUCT_FACTS_INPUT_TOPIC);
    }

    @PostMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public void save(@RequestBody MilkProductFact fact) {
        LOGGER.warn("Trying to save fact {} in database", fact);
        try {
            String sql = "INSERT INTO public.milk_products_facts (year, fluid_milk, fluid_yogurt, butter, cheese_american, cheese_other, cheese_cottage, evap_cnd_canned_whole_milk, evap_cnd_bulk_whole_milk, evap_cnd_bulk_and_can_skim_milk, frozen_ice_cream_regular, frozen_ice_cream_reduced_fat, frozen_sherbet, frozen_other, dry_whole_milk, dry_nonfat_milk, dry_buttermilk, dry_whey) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            jdbcTemplate.update(sql,
                    fact.getYear(),
                    fact.getFluidMilk(),
                    fact.getFluidYogurt(),
                    fact.getButter(),
                    fact.getCheeseAmerican(),
                    fact.getCheeseOther(),
                    fact.getCheeseCottage(),
                    fact.getEvapCndCannedWholeMilk(),
                    fact.getEvapCndBulkWholeMilk(),
                    fact.getEvapCndBulkAndCanSkimMilk(),
                    fact.getFrozenIceCreamRegular(),
                    fact.getFrozenIceCreamReducedFat(),
                    fact.getFrozenSherbet(),
                    fact.getFrozenOther(),
                    fact.getDryWholeMilk(),
                    fact.getDryNonfatMilk(),
                    fact.getDryButtermilk(),
                    fact.getDryWhey());

            LOGGER.info("Successfully inserted object to database");
        } catch (Exception e) {
            LOGGER.error("Error occurred while saving fact to db. Details {}", e.getMessage());
        }
    }
}
