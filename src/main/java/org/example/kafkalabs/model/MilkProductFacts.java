package org.example.kafkalabs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Random;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MilkProductFacts {
    Integer year;
    Integer fluidMilk;
    Double fluidYogurt;
    Double butter;
    Double cheeseAmerican;
    Double cheeseOther;
    Double cheeseCottage;
    Double evapCndCannedWholeMilk;
    Double evapCndBulkWholeMilk;
    Double evapCndBulkAndCanSkimMilk;
    Double frozenIceCreamRegular;
    Double frozenIceCreamReducedFat;
    Double frozenSherbet;
    Double frozenOther;
    Float dryWholeMilk;
    Double dryNonfatMilk;
    Float dryButtermilk;
    Float dryWhey;

    public static MilkProductFacts createRandomObject() {
        Random random = new Random();
        MilkProductFacts randomObject = new MilkProductFacts();
        randomObject.setYear(random.nextInt(1900, 2100));
        randomObject.setFluidMilk(random.nextInt(1000));
        randomObject.setFluidYogurt(random.nextDouble() * 100);
        randomObject.setButter(random.nextDouble() * 100);
        randomObject.setCheeseAmerican(random.nextDouble() * 100);
        randomObject.setCheeseOther(random.nextDouble() * 100);
        randomObject.setCheeseCottage(random.nextDouble() * 100);
        randomObject.setEvapCndCannedWholeMilk(random.nextDouble() * 100);
        randomObject.setFrozenIceCreamRegular(random.nextDouble() * 100);
        randomObject.setEvapCndBulkWholeMilk(random.nextDouble() * 100);
        randomObject.setEvapCndBulkAndCanSkimMilk(random.nextDouble() * 100);
        randomObject.setFrozenIceCreamReducedFat(random.nextDouble() * 100);
        randomObject.setFrozenSherbet(random.nextDouble() * 100);
        randomObject.setFrozenOther(random.nextDouble() * 100);
        randomObject.setDryWholeMilk(random.nextFloat() * 100);
        randomObject.setDryNonfatMilk(random.nextDouble() * 100);
        randomObject.setDryButtermilk(random.nextFloat() * 100);
        randomObject.setDryWhey(random.nextFloat() * 100);
        return randomObject;
    }
}
