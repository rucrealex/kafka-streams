package ru.crealex.kafka.streams.serde;

import ru.crealex.kafka.streams.model.Time;
import ru.crealex.kafka.streams.model.User;
import ru.crealex.kafka.streams.model.UserActivity;
import ru.crealex.kafka.streams.model.WorkTime;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

public class JsonSerde {
    public static final JsonPOJOSerializer<User> USER_SERDE = new JsonPOJOSerializer<>(User.class);
    public static final JsonPOJOSerializer<Time> TIME_SERDE = new JsonPOJOSerializer<>(Time.class);
    public static final JsonPOJOSerializer<WorkTime> WORKTIME_SERDE = new JsonPOJOSerializer<>(WorkTime.class);
    public static final JsonPOJOSerializer<UserActivity> USER_WORKTIME_SERDE = new JsonPOJOSerializer<>(UserActivity.class);
}
