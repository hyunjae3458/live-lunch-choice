package org.example.livelunchchoice.vote;


import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.example.livelunchchoice.vote.redis.VoteRankingRedis;
import org.example.livelunchchoice.vote.redis.VoteRankingPublisher;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
@Profile("realtime")
@RequiredArgsConstructor
public class VoteRealtimeService implements RankingSource {
    private final VoteRankingRedis rankingRedis;
    private final VoteRankingPublisher rankingPublisher;

    // Redis 랭킹 조회: Sorted Set 순서를 그대로 반환하며 DB에 접근하지 않는다.
    public List<VoteResultResponse> rankings() {
        return rankingRedis.findAll();
    }

    // 이벤트 집계: Lua로 중복을 확인하고, 새로 집계한 이벤트만 Pub/Sub으로 알린다.
    public boolean aggregate(VoteCreateEvent event) {
        boolean aggregated = rankingRedis.incrementIfNotProcessed(event.eventId(), event.candidateId());
        if (aggregated) {
            rankingPublisher.publish(event.eventId());
        }
        return aggregated;
    }
}
