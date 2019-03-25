package ru.crealex.kafka.streams.repositories;

import org.springframework.data.repository.CrudRepository;
import ru.crealex.kafka.streams.model.UserSummary;

public interface UserSummaryRepository extends CrudRepository<UserSummary, Long> {
}
