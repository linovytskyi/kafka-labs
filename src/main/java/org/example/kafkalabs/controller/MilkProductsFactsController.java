package org.example.kafkalabs.controller;

import org.example.kafkalabs.model.MilkProductFacts;
import org.example.kafkalabs.service.MilkProductsFactsKafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

@RestController()
@RequestMapping("milk-products-facts")
public class MilkProductsFactsController {

    private final MilkProductsFactsKafkaService milkProductsFactsKafkaService;

    private final JdbcTemplate jdbcTemplate;

    @Value("${kafka.db.connect.topic}")
    private String kafkaDbConnectTopic;

    private static final Logger LOGGER = LoggerFactory.getLogger(MilkProductsFactsController.class);

    public MilkProductsFactsController(MilkProductsFactsKafkaService milkProductsFactsKafkaService,
                                       JdbcTemplate jdbcTemplate) {
        this.milkProductsFactsKafkaService = milkProductsFactsKafkaService;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostMapping("kafka/random")
    @ResponseStatus(HttpStatus.OK)
    public void sendRandomFactToKafka() {
        LOGGER.info("Get request to send random fact to kafka");
        milkProductsFactsKafkaService.sendRandomObject(kafkaDbConnectTopic);
    }

    @PostMapping()
    @ResponseStatus(HttpStatus.CREATED)
    public void save(@RequestBody MilkProductFacts fact) {
        LOGGER.info("Trying to save fact {} in database", fact);
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
