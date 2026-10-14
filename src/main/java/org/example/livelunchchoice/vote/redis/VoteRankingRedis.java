package org.example.livelunchchoice.vote.redis;

import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class VoteRankingRedis {
    private static final String RANKING_KEY = "live-lunch-choice:ranking";
    private static final String PROCESSED_EVENTS_KEY = "live-lunch-choice:processed-events";
    private final StringRedisTemplate stringRedisTemplate;
    private static final DefaultRedisScript<Long> AGGREGATE_SCRIPT = createAggregateScript();

    public boolean incrementIfNotProcessed(String eventId, long candidateId){
        Long result = stringRedisTemplate.execute(
                AGGREGATE_SCRIPT,
                List.of(RANKING_KEY, PROCESSED_EVENTS_KEY),
                eventId,
                Long.toString(candidateId)
        );

        if(result == null){
            throw new IllegalStateException("Redis 집계 결과가 없습니다.");
        }

        return result == 1L;
    }

    private static DefaultRedisScript<Long> createAggregateScript(){
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();

        script.setLocation(new ClassPathResource("redis/aggregate-vote.lua"));
        script.setResultType(Long.class);

        return script;
    }
}
