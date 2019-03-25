package ru.crealex.kafka.streams.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "user_summary")
public class UserSummary implements Serializable {
    @Id
    private Long userId;

    private Long summaryTime;

    @OneToOne
    @PrimaryKeyJoinColumn
    private UserEvent user;

    public UserSummary(Long summaryTime, UserEvent user) {
        this.userId = user.getId();
        this.summaryTime = summaryTime;
        this.user = user;
    }
}
