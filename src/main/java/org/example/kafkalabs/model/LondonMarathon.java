package org.example.kafkalabs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Random;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LondonMarathon {
    private Date date;
    private Integer year;
    private Integer applicants;
    private Integer accepted;
    private Integer starters;
    private Integer finishers;
    private Double raised;
    @JsonProperty("Official charity")
    private String officialCharity;

    public static LondonMarathon createRandomEvent() {
        Random random = new Random();
        Date randomDate = new Date();
        int randomYear = 2020 + random.nextInt(5);
        int randomApplicants = random.nextInt(500) + 100;
        int randomAccepted = random.nextInt(randomApplicants);
        int randomStarters = random.nextInt(randomAccepted);
        int randomFinishers = random.nextInt(randomStarters);
        double randomRaised = random.nextDouble() * 10000;
        String[] charities = {"Charity A", "Charity B", "Charity C", "Charity D", "Charity E"};
        String randomCharity = charities[random.nextInt(charities.length)];


        return new LondonMarathon(randomDate, randomYear, randomApplicants, randomAccepted,
                randomStarters, randomFinishers, randomRaised, randomCharity);
    }
}