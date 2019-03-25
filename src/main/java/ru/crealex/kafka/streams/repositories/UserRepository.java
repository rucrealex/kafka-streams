package ru.crealex.kafka.streams.repositories;

import org.springframework.data.repository.CrudRepository;
import ru.crealex.kafka.streams.model.UserEvent;

public interface UserRepository extends CrudRepository<UserEvent, Long> {
}
