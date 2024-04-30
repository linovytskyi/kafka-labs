package org.example.kafkalabs.model;

import lombok.Data;

import java.util.Random;

@Data
public class MilkCowFact{
    private Integer year;
    private Integer avgMilkCowNumber;
    private Integer milkPerCow;
    private Double milkProductionLbs;
    private Double avgPriceMilk;
    private Double dairyRation;
    private Double milkFeedPriceRatio;
    private Double milkCowCostPerAnimal;
    private Double milkVolumeToBuyCowInLbs;
    private Double alfalfaHayPrice;
    private Double slaughterCowPrice;

    public static MilkCowFact createRandomFact() {
        MilkCowFact milkCowFact = new MilkCowFact();
        Random random = new Random();

        milkCowFact.setYear(random.nextInt(100) + 1920);
        milkCowFact.setAvgMilkCowNumber(random.nextInt(1000));
        milkCowFact.setMilkPerCow(random.nextInt(30));
        milkCowFact.setMilkProductionLbs(random.nextDouble() * 10000);
        milkCowFact.setAvgPriceMilk((double) (random.nextInt(0, 1000)) / 1000);
        milkCowFact.setDairyRation(random.nextDouble() * 10);
        milkCowFact.setMilkFeedPriceRatio(random.nextDouble() * 10);
        milkCowFact.setMilkCowCostPerAnimal(random.nextDouble() * 1000);
        milkCowFact.setMilkVolumeToBuyCowInLbs(random.nextDouble() * 10000);
        milkCowFact.setAlfalfaHayPrice(random.nextDouble() * 100);
        milkCowFact.setSlaughterCowPrice(random.nextDouble() * 1000);

        return milkCowFact;
    }
}

