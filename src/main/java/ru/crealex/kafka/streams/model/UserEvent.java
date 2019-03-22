package ru.crealex.kafka.streams.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserEvent {
    private Long id;
    private String title;
    private String name;
    private Boolean isManager;
}
