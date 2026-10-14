package org.example.livelunchchoice.vote;

import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.example.livelunchchoice.vote.redis.VoteRankingRedis;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class VoteAggregationService {
    private final VoteRankingRedis voteRankingRedis;

    public boolean aggregate(VoteCreateEvent event){
        return voteRankingRedis.incrementIfNotProcessed(event.eventId(),event.candidateId());
    }
}
