package ru.crealex.kafka.streams.model;

import lombok.Data;
import lombok.ToString;

@Data
public class TitleTime {
    private Long id;
    private Long sumHours;
    private Long eventCounts;

    public TitleTime add(WorkTime time) {
        if (time.getTitleId() == null || time.getHours() == null) {
            throw new IllegalArgumentException("Invalid time event: " + String.valueOf(time));
        }

        if (this.id == null) {
            this.setId(time.getTitleId());
        }

        if (this.sumHours == null) {
            this.setSumHours(0L);
        }

        if (this.eventCounts == null) {
            this.setEventCounts(0L);
        }

        this.setEventCounts(this.getEventCounts() + 1);
        this.setSumHours(this.getSumHours() + time.getHours());
        return this;
    }
}
