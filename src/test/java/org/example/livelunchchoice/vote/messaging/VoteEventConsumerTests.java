package org.example.livelunchchoice.vote.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.livelunchchoice.vote.VoteAggregationService;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.example.livelunchchoice.vote.redis.VoteRankingRedis;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(OutputCaptureExtension.class)
class VoteEventConsumerTests {
    private final VoteRankingRedis redis = mock(VoteRankingRedis.class);
    private final VoteEventConsumer consumer = new VoteEventConsumer(new VoteAggregationService(redis));
    private final VoteCreateEvent event = new VoteCreateEvent("event-1", 10L, 2L, Instant.now());
    private final ConsumerRecord<String, VoteCreateEvent> record =
            new ConsumerRecord<>("vote-created", 0, 12L, "10", event);

    @Test
    void successfulAggregationLogsSuccess(CapturedOutput output) {
        when(redis.incrementIfNotProcessed("event-1", 2L)).thenReturn(true);
        consumer.consume(record);
        verify(redis).incrementIfNotProcessed("event-1", 2L);
        assertThat(output.getOut()).contains("VOTE_AGGREGATED eventId=event-1")
                .doesNotContain("VOTE_DUPLICATE_SKIPPED");
    }

    @Test
    void duplicateReturnsNormallyWithoutSuccessLog(CapturedOutput output) {
        when(redis.incrementIfNotProcessed("event-1", 2L)).thenReturn(false);
        assertThatCode(() -> consumer.consume(record)).doesNotThrowAnyException();
        verify(redis, times(1)).incrementIfNotProcessed("event-1", 2L);
        assertThat(output.getOut()).contains("VOTE_DUPLICATE_SKIPPED eventId=event-1")
                .doesNotContain("VOTE_AGGREGATED");
    }

    @Test
    void failurePropagatesToKafkaContainerWithoutSuccessLog(CapturedOutput output) {
        RuntimeException failure = new IllegalStateException("Redis unavailable");
        when(redis.incrementIfNotProcessed("event-1", 2L)).thenThrow(failure);
        assertThatThrownBy(() -> consumer.consume(record)).isSameAs(failure);
        assertThat(output.getOut()).doesNotContain("VOTE_AGGREGATED", "VOTE_DUPLICATE_SKIPPED");
    }
}
