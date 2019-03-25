package ru.crealex.kafka.streams.repositories;

import org.springframework.data.repository.CrudRepository;
import ru.crealex.kafka.streams.model.TimeEvent;

public interface TimeRepository extends CrudRepository<TimeEvent, Long> {
}
