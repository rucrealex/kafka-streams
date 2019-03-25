package ru.crealex.kafka.streams.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "time_events")
public class TimeEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long eventId;
    private Long userId;
    private Long hours;

    public TimeEvent addTime(TimeEvent time) {
        if (this.userId == null) {
            this.userId = time.getUserId();
            this.hours = 0L;
        }
        this.hours = this.hours + time.getHours();
        return this;
    }
}
