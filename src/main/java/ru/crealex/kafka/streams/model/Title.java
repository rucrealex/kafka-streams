package ru.crealex.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Title {
    private Long id;
    private String name;
    private String role;
    @JsonProperty("isManager")
    private boolean isManager;
}
