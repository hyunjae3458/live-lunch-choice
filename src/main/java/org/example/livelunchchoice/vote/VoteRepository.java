package org.example.livelunchchoice.vote;

import org.example.livelunchchoice.vote.projection.VoteResultProjection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface VoteRepository extends JpaRepository<Vote, Long> {
    boolean existsByUserId(String userId);

    // 비교용 실시간 계약: 이름과 0표 후보는 후보 목록 API에서 결합한다.
    @Query("""
            select v.candidate.id as candidateId, count(v.id) as voteCount
            from Vote v group by v.candidate.id
            order by count(v.id) desc, v.candidate.id asc
            """)
    List<org.example.livelunchchoice.vote.projection.VoteCountProjection> findRankingCounts();

    @Query("""
            select c.id as candidateId, c.name as name, count(v.id) as voteCount
            from Candidate c left join Vote v on v.candidate = c
            group by c.id, c.name
            order by count(v.id) desc, c.id asc
            """)
    List<VoteResultProjection> findResults();
}
