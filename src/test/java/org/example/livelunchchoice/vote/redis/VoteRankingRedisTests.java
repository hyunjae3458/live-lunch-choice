package org.example.livelunchchoice.vote.redis;

import org.junit.jupiter.api.*;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

@Tag("redis")
@Testcontainers
class VoteRankingRedisTests {
    private static final String RANKING = "live-lunch-choice:ranking";
    private static final String PROCESSED = "live-lunch-choice:processed-events";
    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:8"))
            .withExposedPorts(6379);
    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate template;
    private VoteRankingRedis ranking;

    @BeforeEach
    void setup() {
        // Always connect to our disposable container, never the application Redis.
        connectionFactory = new LettuceConnectionFactory(redis.getHost(), redis.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        template = new StringRedisTemplate(connectionFactory);
        template.delete(List.of(RANKING, PROCESSED));
        ranking = new VoteRankingRedis(template);
    }

    @AfterEach
    void cleanup() {
        if (connectionFactory != null) connectionFactory.destroy();
    }

    @Test
    void rankingIsEmptyWithoutScores() {
        assertThat(ranking.findAll()).isEmpty();
    }

    @Test
    void rankingPreservesRedisScoreAndReverseLexicalTieOrder() {
        template.opsForZSet().add(RANKING, "1", 4);
        template.opsForZSet().add(RANKING, "2", 3);
        template.opsForZSet().add(RANKING, "10", 3);
        assertThat(ranking.findAll()).containsExactly(
                new VoteResultResponse(1, 4), new VoteResultResponse(2, 3), new VoteResultResponse(10, 3));
    }

    @Test
    void duplicateIsSkippedEvenAfterClientIsRecreated() {
        assertThat(ranking.incrementIfNotProcessed("event-1", 1)).isTrue();
        VoteRankingRedis restartedClient = new VoteRankingRedis(template);
        assertThat(restartedClient.incrementIfNotProcessed("event-1", 1)).isFalse();
        assertThat(template.opsForZSet().score(RANKING, "1")).isEqualTo(1.0);
        assertThat(template.opsForSet().members(PROCESSED)).containsExactly("event-1");
    }

    @Test
    void distinctEventsCountPerCandidate() {
        assertThat(ranking.incrementIfNotProcessed("a", 1)).isTrue();
        assertThat(ranking.incrementIfNotProcessed("b", 1)).isTrue();
        assertThat(ranking.incrementIfNotProcessed("c", 2)).isTrue();
        assertThat(template.opsForZSet().score(RANKING, "1")).isEqualTo(2.0);
        assertThat(template.opsForZSet().score(RANKING, "2")).isEqualTo(1.0);
        assertThat(template.opsForSet().size(PROCESSED)).isEqualTo(3L);
    }

    @Test
    void concurrentDuplicatesIncrementOnlyOnce() throws Exception {
        List<Boolean> results = concurrently(false);
        assertThat(results.stream().filter(Boolean::booleanValue).count()).isEqualTo(1L);
        assertThat(template.opsForZSet().score(RANKING, "1")).isEqualTo(1.0);
        assertThat(template.opsForSet().size(PROCESSED)).isEqualTo(1L);
    }

    @Test
    void concurrentDistinctEventsDoNotLoseIncrements() throws Exception {
        assertThat(concurrently(true)).containsOnly(true).hasSize(16);
        assertThat(template.opsForZSet().score(RANKING, "1")).isEqualTo(16.0);
        assertThat(template.opsForSet().size(PROCESSED)).isEqualTo(16L);
    }

    @Test
    void wrongRankingTypeDoesNotMarkEventAndCanBeRetried() {
        template.opsForValue().set(RANKING, "wrong-type");
        assertThatThrownBy(() -> ranking.incrementIfNotProcessed("a", 1)).isInstanceOf(RuntimeException.class);
        assertThat(template.hasKey(PROCESSED)).isFalse();
        template.delete(RANKING);
        assertThat(ranking.incrementIfNotProcessed("a", 1)).isTrue();
        assertThat(template.opsForZSet().score(RANKING, "1")).isEqualTo(1.0);
    }

    @Test
    void wrongProcessedTypeFailsBeforeIncrement() {
        template.opsForValue().set(PROCESSED, "wrong-type");
        assertThatThrownBy(() -> ranking.incrementIfNotProcessed("a", 1)).isInstanceOf(RuntimeException.class);
        assertThat(template.hasKey(RANKING)).isFalse();
        assertThat(template.opsForValue().get(PROCESSED)).isEqualTo("wrong-type");
    }

    private List<Boolean> concurrently(boolean distinct) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(16);
        CountDownLatch ready = new CountDownLatch(16);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 16; i++) {
                String eventId = distinct ? "event-" + i : "same-event";
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("start timeout");
                    return ranking.incrementIfNotProcessed(eventId, 1);
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            List<Boolean> results = new ArrayList<>();
            for (Future<Boolean> future : futures) results.add(future.get(15, TimeUnit.SECONDS));
            return results;
        } finally {
            start.countDown();
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
