package org.example.livelunchchoice.vote.redis;

import org.springframework.context.annotation.Profile;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@RequiredArgsConstructor
@Profile("realtime")
public class VoteRankingRedis {
    private static final String RANKING_KEY = "live-lunch-choice:ranking";
    private static final String PROCESSED_EVENTS_KEY = "live-lunch-choice:processed-events";
    private final StringRedisTemplate stringRedisTemplate;
    private static final DefaultRedisScript<Long> AGGREGATE_SCRIPT = createAggregateScript();

    public List<VoteResultResponse> findAll() {
        Set<ZSetOperations.TypedTuple<String>> rankings =
                stringRedisTemplate.opsForZSet().reverseRangeWithScores(RANKING_KEY, 0, -1);
        if (rankings == null) {
            throw new IllegalStateException("Redis 랭킹 조회 결과가 없습니다.");
        }
        return rankings.stream()
                .map(tuple -> new VoteResultResponse(Long.parseLong(tuple.getValue()), tuple.getScore().longValue()))
                .toList();
    }

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
