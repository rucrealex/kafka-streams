package ru.crealex.kafka.streams.serde;

import ru.crealex.kafka.streams.model.UserEvent;
import ru.crealex.kafka.streams.model.UserSummary;
import ru.crealex.kafka.streams.model.TimeEvent;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

public class JsonSerde {
    public static final JsonPOJOSerializer<UserEvent> USER_EVENT_SERDE = new JsonPOJOSerializer<>(UserEvent.class);
    public static final JsonPOJOSerializer<TimeEvent> TIME_EVENT_SERDE = new JsonPOJOSerializer<>(TimeEvent.class);
    public static final JsonPOJOSerializer<UserSummary> USER_ACTITITY_SERDE = new JsonPOJOSerializer<>(UserSummary.class);
}
