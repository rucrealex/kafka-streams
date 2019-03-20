package ru.crealex.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.ToString;

@Data
public class Title {
    private Long id;
    private String name;
    private String role;
    private Boolean isManager;
}
