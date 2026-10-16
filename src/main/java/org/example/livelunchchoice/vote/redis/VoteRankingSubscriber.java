package org.example.livelunchchoice.vote.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.livelunchchoice.vote.sse.VoteRankingSseService;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class VoteRankingSubscriber implements MessageListener {
    private final VoteRankingSseService sseService;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String eventId = new String(message.getBody(), StandardCharsets.UTF_8);
        log.info("RANKING_CHANGE_RECEIVED eventId={}", eventId);
        sseService.broadcast();
    }
}
