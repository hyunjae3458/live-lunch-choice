package org.example.livelunchchoice.vote.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class VoteRankingPublisher {
    public static final String CHANNEL = "live-lunch-choice:ranking-changed";
    private final StringRedisTemplate redisTemplate;

    public void publish(String eventId) {
        redisTemplate.convertAndSend(CHANNEL, eventId);
        log.info("RANKING_CHANGE_PUBLISHED eventId={}", eventId);
    }
}
