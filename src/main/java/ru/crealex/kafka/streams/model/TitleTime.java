package ru.crealex.kafka.streams.model;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
public class TitleTime extends Title {
    private Long sumHours;
}
