package ru.crealex.kafka.streams.model;

import lombok.Data;

@Data
public class SummaryTime {
    private Long id;
    private Long sumHours;
    private Long eventCounts;

    public SummaryTime add(TimeEvent time) {
        if (time.getUserId() == null || time.getHours() == null) {
            throw new IllegalArgumentException("Invalid summaryTime event: " + String.valueOf(time));
        }

        if (this.id == null) {
            this.setId(time.getUserId());
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
