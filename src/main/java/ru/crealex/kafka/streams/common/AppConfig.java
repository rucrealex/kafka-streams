package ru.crealex.kafka.streams.common;

import ru.crealex.kafka.streams.model.TimeEvent;

public class AppConfig {
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TITLE_TEAM_LEAD = "Team Lead";
    public static final String TITLE_PRODUCT_OWNER = "Product Owner";
    public static final String TITLE_DEVELOPER = "Developer";
    public static final String TITLE_QA_ENGENEER = "QA Engeneer";

    public static final String TOPIC_USER = "users";
    public static final String TOPIC_TIME = "times";
    public static final Integer PARTITION_COUNT = 0;
}
