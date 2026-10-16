package org.example.livelunchchoice.vote.redis;

import org.springframework.context.annotation.Profile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("realtime")
public class VoteRankingPublisher {
    public static final String CHANNEL = "live-lunch-choice:ranking-changed";
    private final StringRedisTemplate redisTemplate;

    public void publish(String eventId) {
        redisTemplate.convertAndSend(CHANNEL, eventId);
        log.info("RANKING_CHANGE_PUBLISHED eventId={}", eventId);
    }
}
