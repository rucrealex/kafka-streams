package ru.crealex.kafka.streams.bootstrap;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.crealex.kafka.streams.model.Time;
import ru.crealex.kafka.streams.model.User;
import ru.crealex.kafka.streams.model.UserActivity;
import ru.crealex.kafka.streams.model.WorkTime;
import ru.crealex.kafka.streams.serde.JsonSerde;
import ru.crealex.kafka.streams.utility.JsonPOJOSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class Bootstrap {

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "titles_app1" /*+ Math.round(100000 * Math.random())*/);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/work/tmp/kafka-stream");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JsonPOJOSerializer.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, User> users = builder.table("users", Consumed.with(Serdes.String(), JsonSerde.USER_SERDE));
        final KStream<String, WorkTime> times = builder.stream("times", Consumed.with(Serdes.String(), JsonSerde.WORKTIME_SERDE));

//        KStream<String, WorkTime> worksTimesWithKey = workTimes
//                .filter((key, workTime) -> workTime != null)
//                .selectKey((key, workTime) -> String.valueOf(workTime.getTitleId()));

//        users.filter((key, value) -> value != null)
//                .foreach((key, value) -> log.debug("prn key: " + key + ", value:" + String.valueOf(value)));


//        KStream<String, User> titlesWithKeys = users
//                .filter((key, title) -> title != null)
//                .selectKey((key, title) -> String.valueOf(title.getId()));

//        times.filter((key, value) -> value != null)
//                .foreach((key, value) -> log.debug("prn key: " + key + ", value:" + String.valueOf(value)));



        KStream<String, Time> userTimes = times.groupByKey()
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(5)))
                .aggregate(Time::new, ((key, value, aggregateTime) -> aggregateTime.add(value)),
                        Materialized.<String, Time, WindowStore<Bytes, byte[]>>as("times-aggregates")
                                .withValueSerde(new JsonPOJOSerializer<>(Time.class)))
                .toStream()
                .selectKey(new KeyValueMapper<Windowed<String>, Time, String>() {
                    @Override
                    public String apply(Windowed<String> key, Time value) {
                        return String.valueOf(value.getId());
                    }
                });

        //        userTimes.to("times-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        userTimes.to("times-output");

        KStream<String, UserActivity> joined = userTimes.leftJoin(users, new ValueJoiner<Time, User, UserActivity>() {
            @Override
            public UserActivity apply(Time time, User user) {
                return new UserActivity(user, time);
            }
        }, Joined.with(Serdes.String(), JsonSerde.TIME_SERDE, JsonSerde.USER_SERDE));
//
        joined.to("users-output", Produced.valueSerde(JsonSerde.USER_WORKTIME_SERDE));

//        userTimes.filter((key, value) -> value != null)
//                .foreach((key, value) -> log.debug("prn key: " + key + ", value:" + String.valueOf(value)));


//        KStream<String, User> titlesOutput = users.groupByKey().reduce(
//                (aggVal, newVal) -> {
//                    log.debug("aggVal:" + aggVal);
//                    log.debug("newVal:" + newVal);
//                    return newVal;
//                }, Materialized.with(Serdes.String(), new JsonPOJOSerializer<>(User.class))
//        ).toStream();
//
//        titlesOutput.to("users-output2");

//        KTable<String, WorkTime> reduce = times.filter((key, value) -> value != null)
//                .groupByKey()
//                .reduce((value1, value2) -> {
//
//                });
//                .
//                .reduce((value1, value2) -> {
//                    WorkTime workTime = new WorkTime();
//                    if(value1 != null && value2 != null) {
//                        workTime.setHours(value1.getHours() + value2.getHours());
//                    } else {
//                        workTime.setHours(value1.getHours());
//                    }
//                    return workTime;
//                });
//        reduce.toStream().to("times-output");


//        KStream<String, Time> joined = users.join(times, (title, time) -> {
//                    log.debug("time: " + String.valueOf(time));
//                    log.debug("title: " + String.valueOf(title));
//                    if (title == null || time == null) {
//                        return null;
//                    }
//
//                    if (title.getId().equals(time.getTitleId())) {
//                        Time titleTime = new Time();
//                        titleTime.setId(title.getId());
//                        titleTime.setName(title.getName());
//                        titleTime.setTitle(title.getTitle());
//                        titleTime.setIsManager(title.getIsManager());
//                        titleTime.setSumHours(time.getHours());
//                        return titleTime;
//                    }
//                    return null;
//                }, JoinWindows.of(TimeUnit.MINUTES.toMillis(1)),
//                Joined.with(Serdes.String(), new JsonPOJOSerializer<>(User.class), new JsonPOJOSerializer<>(WorkTime.class)));


//        worksTimesWithKey.join(titlesWithKeys, (time, title) -> {
//                    Time titleTime = new Time();
//                    log.debug(String.valueOf(time));
//                    log.debug(String.valueOf(title));
//                    return
//                    titleTime.setId(title.getId());
//                    titleTime.setName(title.getName());
//                    if(time != null) {
//                        titleTime.setSumHours(time.getHours());
//                    } else {
//                        titleTime.setSumHours(0L);
//                    }
//
//                    return titleTime;
//                },
//                JoinWindows.of(TimeUnit.SECONDS.toDays(7)),
//                Joined.with(Serdes.String(), new JsonPOJOSerializer<>(WorkTime.class), new JsonPOJOSerializer<>(User.class)));

//        joined.foreach((key, value) -> log.debug("joined: " + key + " " + String.valueOf(value)));
//
//        joined.to("users-output");



        Topology topology = builder.build();
        log.info(topology.describe().toString());
        KafkaStreams streams = new KafkaStreams(topology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.debug("close streams ...");
            streams.close();
            log.debug("done!");
        }));

        streams.cleanUp();
        streams.start();
    }
}
