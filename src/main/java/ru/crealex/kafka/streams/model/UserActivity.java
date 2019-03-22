package ru.crealex.kafka.streams.model;

import lombok.Data;

@Data
public class UserActivity {
    private UserEvent user;
    private SummaryTime summaryTime;

    public UserActivity() {
    }

    public UserActivity(UserEvent user, SummaryTime summaryTime) {
        this.user = user;
        this.summaryTime = summaryTime;
    }
}
