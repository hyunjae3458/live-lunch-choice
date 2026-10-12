package org.example.livelunchchoice.vote;

import org.example.livelunchchoice.vote.projection.VoteResultProjection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface VoteRepository extends JpaRepository<Vote, Long> {
    boolean existsByUserId(String userId);

    @Query("""
            select c.id as candidateId, c.name as name, count(v.id) as voteCount
            from Candidate c left join Vote v on v.candidate = c
            group by c.id, c.name
            order by count(v.id) desc, c.id asc
            """)
    List<VoteResultProjection> findResults();
}
