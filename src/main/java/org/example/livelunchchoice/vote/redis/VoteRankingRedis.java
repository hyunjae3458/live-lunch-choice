package org.example.livelunchchoice.vote.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class VoteRankingRedis {
    private static final String RANKING_KEY = "live-lunch-choice:ranking";
    private final StringRedisTemplate stringRedisTemplate;

    public double increment(long candidateId){
        Double score = stringRedisTemplate.opsForZSet()
                .incrementScore(
                        RANKING_KEY,
                        Long.toString(candidateId),
                        1.0
                );

        if(score == null){
            throw new IllegalStateException("Redis 집계 결과가 없습니다.");
        }

        return score;
    }
}
