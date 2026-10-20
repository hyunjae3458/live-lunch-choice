package org.example.livelunchchoice.vote;

import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

@Service
@Profile("db-sse")
@RequiredArgsConstructor
public class DbRankingService implements RankingSource {
    private final VoteRepository repository;

    // 커밋된 결과를 별도 읽기 트랜잭션으로 조회한다. 쓰기 트랜잭션을 재사용하지 않는다.
    @Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
    public List<VoteResultResponse> rankings() {
        return repository.findRankingCounts().stream()
                .map(row -> new VoteResultResponse(row.getCandidateId(), row.getVoteCount())).toList();
    }
}
