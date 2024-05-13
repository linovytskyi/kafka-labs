package org.example.kafkalabs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Winner {
    private String category;
    private Integer year;
    private String athlete;
    private String nationality;
    private String time;
}

