package org.example.livelunchchoice.candidate;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(uniqueConstraints = @UniqueConstraint(name = "uk_candidate_name", columnNames = "name"))
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class Candidate {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(nullable = false)
    private String name;

    public Candidate(String name){
        this.name = name;
    }
}
