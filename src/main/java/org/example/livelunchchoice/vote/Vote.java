package org.example.livelunchchoice.vote;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.example.livelunchchoice.candidate.Candidate;

import java.time.Instant;

@Entity
@Table(name = "votes", uniqueConstraints = @UniqueConstraint(name = "uk_vote_user_id", columnNames = "user_id"))
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Vote {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "user_id", nullable = false)
    private String userId;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "candidate_id", nullable = false,
            foreignKey = @ForeignKey(name = "fk_vote_candidate"))
    private Candidate candidate;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    public Vote(String userId, Candidate candidate) {
        this.userId = userId;
        this.candidate = candidate;
        this.createdAt = Instant.now();
    }
}
