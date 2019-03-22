package ru.crealex.kafka.streams.serde;

import ru.crealex.kafka.streams.model.SummaryTime;
import ru.crealex.kafka.streams.model.UserEvent;
import ru.crealex.kafka.streams.model.UserActivity;
import ru.crealex.kafka.streams.model.TimeEvent;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

public class JsonSerde {
    public static final JsonPOJOSerializer<UserEvent> USER_EVENT_SERDE = new JsonPOJOSerializer<>(UserEvent.class);
    public static final JsonPOJOSerializer<TimeEvent> TIME_EVENT_SERDE = new JsonPOJOSerializer<>(TimeEvent.class);
    public static final JsonPOJOSerializer<SummaryTime> SUMMARY_TIME_SERDE = new JsonPOJOSerializer<>(SummaryTime.class);
    public static final JsonPOJOSerializer<UserActivity> USER_ACTITITY_SERDE = new JsonPOJOSerializer<>(UserActivity.class);
}
