package ru.crealex.kafka.streams.model;

import lombok.Data;

@Data
public class UserActivity {
    private User user;
    private Time time;

    public UserActivity() {
    }

    public UserActivity(User user, Time time) {
        this.user = user;
        this.time = time;
    }
}
