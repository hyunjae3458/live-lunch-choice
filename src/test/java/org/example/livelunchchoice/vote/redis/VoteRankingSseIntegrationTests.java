package org.example.livelunchchoice.vote.redis;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.example.livelunchchoice.vote.VoteRepository;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.example.livelunchchoice.vote.messaging.VoteEventPublisher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@Tag("redis")
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"realtime", "ranking-e2e"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@Import(VoteRankingSseIntegrationTests.EventCaptureConfig.class)
class VoteRankingSseIntegrationTests {
    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:8").withExposedPorts(6379);
    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.1.2");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Value("${local.server.port}") int port;
    @Autowired ObjectMapper mapper;
    @Autowired VoteRepository votes;
    @Autowired EventCapture events;
    @Autowired KafkaTemplate<String, VoteCreateEvent> kafkaTemplate;
    @Autowired RedisMessageListenerContainer rankingListenerContainer;
    @MockitoSpyBean VoteRankingPublisher publisher;
    @MockitoSpyBean VoteRankingSubscriber subscriber;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    @Test
    void realHttpVoteKafkaRedisPubSubAndTwoSseConnections() throws Exception {
        assertThat(get("/api/mode").body()).isEqualTo("{\"mode\":\"realtime\"}");
        assertThat(get("/").body()).contains("id=\"vote-form\"");
        // isListening is set only after the subscription acknowledgement has arrived.
        await().atMost(Duration.ofSeconds(15)).until(rankingListenerContainer::isListening);
        assertThat(get("/api/votes/rankings").body()).isEqualTo("[]");
        try (SseConnection first = openStream(); SseConnection second = openStream()) {
            assertThat(first.next()).isEqualTo("[]");
            assertThat(second.next()).isEqualTo("[]");
            HttpResponse<String> candidate = post("/api/candidates", "{\"name\":\"SSE 통합검증\"}");
            assertThat(candidate.statusCode()).isEqualTo(201);
            long candidateId = mapper.readTree(candidate.body()).get("id").asLong();
            HttpResponse<String> vote = post("/api/votes", "{\"userId\":\"sse-integration-user\",\"candidateId\":" + candidateId + "}");
            assertThat(vote.statusCode()).isEqualTo(201);
            VoteCreateEvent event = events.queue.poll(15, TimeUnit.SECONDS);
            assertThat(event).isNotNull();
            String firstUpdate = first.next();
            String secondUpdate = second.next();
            JsonNode expected = mapper.readTree("[{\"candidateId\":" + candidateId + ",\"voteCount\":1}]");
            assertThat(mapper.readTree(firstUpdate)).isEqualTo(expected);
            assertThat(mapper.readTree(secondUpdate)).isEqualTo(expected);
            assertThat(mapper.readTree(get("/api/votes/rankings").body())).isEqualTo(expected);
            assertThat(votes.count()).isEqualTo(1);
            assertThat(mapper.readTree(get("/api/votes/results").body()).get(0).get("voteCount").asLong()).isEqualTo(1);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                verify(publisher, times(1)).publish(event.eventId());
                verify(subscriber, times(1)).onMessage(any(), any());
            });

            // Same eventId is deliberately sent again through the real broker.
            long duplicateOffset = kafkaTemplate.send(VoteEventPublisher.TOPIC,
                    Long.toString(event.voteId()), event).get(15, TimeUnit.SECONDS).getRecordMetadata().offset();
            try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
                await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                    long committed = admin.listConsumerGroupOffsets("vote-log-group").partitionsToOffsetAndMetadata()
                            .get(5, TimeUnit.SECONDS).get(new TopicPartition(VoteEventPublisher.TOPIC, 0)).offset();
                    assertThat(committed).isEqualTo(duplicateOffset + 1);
                });
            }
            assertThat(mapper.readTree(get("/api/votes/rankings").body())).isEqualTo(expected);
            verify(publisher, times(1)).publish(event.eventId());
            assertThat(first.messages.poll(750, TimeUnit.MILLISECONDS)).isNull();
            assertThat(second.messages.poll(750, TimeUnit.MILLISECONDS)).isNull();
            verify(subscriber, times(1)).onMessage(any(), any());
            first.close();
            try (SseConnection reconnected = openStream()) {
                assertThat(mapper.readTree(reconnected.next())).isEqualTo(expected);
            }
            System.out.printf("SSE_E2E eventId=%s voteId=%d candidateId=%d duplicateOffset=%d committed=%d initial=[] first=%s second=%s score=0->1->1 HTTP(candidate/vote/rankings/results)=201/201/200/200 DBvotes=%d pubsub=1 reconnect=latest%n",
                    event.eventId(), event.voteId(), candidateId, duplicateOffset, duplicateOffset + 1, firstUpdate, secondUpdate, votes.count());
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(uri(path)).timeout(Duration.ofSeconds(15)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response;
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(uri(path)).timeout(Duration.ofSeconds(15))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) { return URI.create("http://127.0.0.1:" + port + path); }

    private SseConnection openStream() throws Exception {
        HttpResponse<InputStream> response = client.sendAsync(HttpRequest.newBuilder(uri("/api/votes/rankings/stream"))
                .header("Accept", "text/event-stream").GET().build(), HttpResponse.BodyHandlers.ofInputStream()).get(15, TimeUnit.SECONDS);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("text/event-stream");
        return new SseConnection(response.body());
    }

    static class SseConnection implements AutoCloseable {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>();
        final InputStream input;
        final Thread reader;
        SseConnection(InputStream input) {
            this.input = input;
            reader = new Thread(() -> {
                try (BufferedReader lines = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                    String name = "";
                    String data = "";
                    String line;
                    while ((line = lines.readLine()) != null) {
                        if (line.startsWith("event:")) name = line.substring(6).trim();
                        else if (line.startsWith("data:")) data = line.substring(5).trim();
                        else if (line.isEmpty()) {
                            if ("ranking".equals(name)) messages.add(data);
                            name = ""; data = "";
                        }
                    }
                } catch (IOException ignored) { /* Closing the HTTP stream ends the reader. */ }
            }, "sse-test-reader");
            reader.setDaemon(true);
            reader.start();
        }
        String next() throws InterruptedException {
            String value = messages.poll(15, TimeUnit.SECONDS);
            assertThat(value).as("ranking SSE within 15 seconds").isNotNull();
            return value;
        }
        @Override public void close() throws IOException { input.close(); }
    }

    @TestConfiguration
    static class EventCaptureConfig {
        @Bean EventCapture eventCapture() { return new EventCapture(); }
    }
    static class EventCapture {
        final BlockingQueue<VoteCreateEvent> queue = new LinkedBlockingQueue<>();
        @EventListener public void onVote(VoteCreateEvent event) { queue.add(event); }
    }
}
