package ru.crealex.kafka.streams.common;

import lombok.extern.slf4j.Slf4j;
import ru.crealex.kafka.streams.model.TimeEvent;

import java.util.Random;

@Slf4j
public class TimeProducer<K, V> {

    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();

        //Send time events 100 times each
        //1:{"userId": 1, "hours": 1}
        //2:{"userId": 2, "hours": 1}
        //3:{"userId": 3, "hours": 1}
        //4:{"userId": 4, "hours": 1}
        //...
        //10:{"userId": 10, "hours": 1}

        Producer<String, TimeEvent> timeProducer = new Producer<>(AppConfig.TOPIC_TIME);
        for (int i = 1; i <= 100; i++) {
            for (int j = 1; j <= 10; j++) {
//                long hours = Math.abs(random.nextLong()) % 8 + 1;
                long hours = 1L;
                String key = String.valueOf(j);
                timeProducer.sendEvent(key , new TimeEvent(0L, Integer.valueOf(j).longValue(), hours));
            }
        }
        timeProducer.close();
    }
}
