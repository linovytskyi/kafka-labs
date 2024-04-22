package org.example.kafkalabs.config;

import com.opencsv.bean.HeaderColumnNameTranslateMappingStrategy;
import org.example.kafkalabs.model.MilkProductFacts;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class CsvToObjectConfig {

    @Bean
    public HeaderColumnNameTranslateMappingStrategy<MilkProductFacts> milkProductFactsToCsvMapping() {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("year", "year");
        mapping.put("fluid_milk", "fluidMilk");
        mapping.put("fluid_yogurt", "fluidYogurt");
        mapping.put("butter", "butter");
        mapping.put("cheese_american", "cheeseAmerican");
        mapping.put("cheese_other", "cheeseOther");
        mapping.put("cheese_cottage", "cheeseCottage");
        mapping.put("evap_cnd_canned_whole_milk", "evapCndCannedWholeMilk");
        mapping.put("evap_cnd_bulk_whole_milk", "evapCndBulkWholeMilk");
        mapping.put("evap_cnd_bulk_and_can_skim_milk", "evapCndBulkAndCanSkimMilk");
        mapping.put("frozen_ice_cream_regular", "frozenIceCreamRegular");
        mapping.put("frozen_ice_cream_reduced_fat", "frozenIceCreamReducedFat");
        mapping.put("frozen_sherbet", "frozenSherbet");
        mapping.put("frozen_other", "frozenOther");
        mapping.put("dry_whole_milk", "dryWholeMilk");
        mapping.put("dry_nonfat_milk", "dryNonFatMilk");
        mapping.put("dry_buttermilk", "dryButterMilk");
        mapping.put("dry_whey", "dryWhey");

        HeaderColumnNameTranslateMappingStrategy<MilkProductFacts> strategy = new HeaderColumnNameTranslateMappingStrategy<>();
        strategy.setColumnMapping(mapping);
        strategy.setType(MilkProductFacts.class);
        return strategy;
    }
}
