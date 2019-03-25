package ru.crealex.kafka.streams.common;

import lombok.extern.slf4j.Slf4j;
import ru.crealex.kafka.streams.model.UserEvent;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UserProducer<K, V> {

    public static void main(String[] args) throws InterruptedException {
        //Send user events
        Producer<String, UserEvent> userProducer = new Producer<>(AppConfig.TOPIC_USER);

        List<UserEvent> users = new ArrayList<>();
        users.add(new UserEvent(1L, AppConfig.TITLE_TEAM_LEAD, "Jhon Snow", true));
        users.add(new UserEvent(2L, AppConfig.TITLE_DEVELOPER, "Billy Bonce", false));
        users.add(new UserEvent(3L, AppConfig.TITLE_DEVELOPER, "Jim Harris", false));
        users.add(new UserEvent(4L, AppConfig.TITLE_PRODUCT_OWNER, "Sandra Black", true));
        users.add(new UserEvent(5L, AppConfig.TITLE_DEVELOPER, "Tom Clifford", false));
        users.add(new UserEvent(6L, AppConfig.TITLE_DEVELOPER, "Bill Jefferson", false));

//        users.add(new UserEvent(7L, AppConfig.TITLE_QA_ENGENEER, "Monty Kramer", false));
//        users.add(new UserEvent(8L, AppConfig.TITLE_QA_ENGENEER, "Jefry MacCornic", false));
//        users.add(new UserEvent(9L, AppConfig.TITLE_DEVELOPER, "Leyla Anderson", false));
//        users.add(new UserEvent(10L, AppConfig.TITLE_DEVELOPER, "Jim Kelly", false));


        users.forEach(userEvent -> {
            userProducer.sendEvent(String.valueOf(userEvent.getId()), userEvent);
        });
        userProducer.close();
    }
}
