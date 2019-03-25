package ru.crealex.kafka.streams.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "user_events")
public class UserEvent {

    @Id
    @Column(name = "user_id", unique = true, nullable = false)
    private Long id;
    private String title;
    private String name;
    private Boolean isManager;
}
