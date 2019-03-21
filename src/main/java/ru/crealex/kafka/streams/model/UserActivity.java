package ru.crealex.kafka.streams.model;

import lombok.Data;

@Data
public class UserActivity {
    private Long id;
    private String name;
    private Long hours;
}
