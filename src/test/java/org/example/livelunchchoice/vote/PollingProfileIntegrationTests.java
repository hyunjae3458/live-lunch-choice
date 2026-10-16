package org.example.livelunchchoice.vote;

import org.example.livelunchchoice.vote.event.VotedCreatedEventListener;
import org.example.livelunchchoice.vote.messaging.*;
import org.example.livelunchchoice.vote.messaging.local.LocalConsumerController;
import org.example.livelunchchoice.vote.redis.*;
import org.example.livelunchchoice.vote.sse.VoteRankingSseService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"local", "polling"})
class PollingProfileIntegrationTests {
    // These sockets run no Kafka/Redis protocol. Any connection attempt is observable.
    private static final ServerSocket kafkaProbe = probe();
    private static final ServerSocket redisProbe = probe();

    private static ServerSocket probe() {
        try { return new ServerSocket(0, 10, InetAddress.getLoopbackAddress()); }
        catch (IOException exception) { throw new IllegalStateException(exception); }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", () -> "127.0.0.1:" + kafkaProbe.getLocalPort());
        properties.add("spring.data.redis.host", () -> "127.0.0.1");
        properties.add("spring.data.redis.port", redisProbe::getLocalPort);
    }

    @AfterAll static void closeProbes() throws IOException { kafkaProbe.close(); redisProbe.close(); }
    @Value("${local.server.port}") int port;
    @Autowired ApplicationContext context;
    @Autowired ObjectMapper mapper;
    @Autowired VoteRepository votes;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void dbHttpWorksWithoutKafkaRedisAndRealtimeApisAreAbsent() throws Exception {
        for (Class<?> type : new Class<?>[]{VoteRealtimeService.class, VoteRankingController.class,
                VoteRankingSseService.class, VoteRankingRedis.class, VoteRankingPublisher.class,
                VoteRankingSubscriber.class, VoteRankingRedisConfig.class, VotedCreatedEventListener.class,
                VoteEventPublisher.class, VoteEventConsumer.class, VoteKafkaConfig.class, LocalConsumerController.class,
                KafkaTemplate.class, KafkaListenerEndpointRegistry.class, RedisConnectionFactory.class,
                RedisMessageListenerContainer.class}) {
            assertThat(context.getBeansOfType(type)).as(type.getSimpleName()).isEmpty();
        }
        HttpResponse<String> candidate = request("POST", "/api/candidates", "{\"name\":\"polling 확인\"}");
        assertThat(candidate.statusCode()).isEqualTo(201);
        long id = mapper.readTree(candidate.body()).get("id").asLong();
        assertThat(request("GET", "/api/candidates", "").statusCode()).isEqualTo(200);
        String vote = "{\"userId\":\"polling-user\",\"candidateId\":" + id + "}";
        assertThat(request("POST", "/api/votes", vote).statusCode()).isEqualTo(201);
        assertThat(request("POST", "/api/votes", vote).statusCode()).isEqualTo(409);
        HttpResponse<String> results = request("GET", "/api/votes/results", "");
        assertThat(results.statusCode()).isEqualTo(200);
        assertThat(mapper.readTree(results.body()).get(0).get("voteCount").asLong()).isEqualTo(1);
        assertThat(votes.count()).isEqualTo(1);
        for (String path : new String[]{"/api/votes/rankings", "/api/votes/rankings/stream", "/api/local/kafka/consumer"}) {
            assertThat(request("GET", path, "").statusCode()).as(path).isEqualTo(404);
        }
        assertThat(request("POST", "/api/local/kafka/consumer/stop", "").statusCode()).isEqualTo(404);
        assertThat(request("POST", "/api/local/kafka/consumer/start", "").statusCode()).isEqualTo(404);
        for (ServerSocket probe : new ServerSocket[]{kafkaProbe, redisProbe}) {
            probe.setSoTimeout(300);
            assertThatThrownBy(() -> {
                try (Socket unexpected = probe.accept()) {
                    throw new AssertionError("Unexpected external-service connection");
                }
            }).isInstanceOf(SocketTimeoutException.class);
        }
        System.out.println("POLLING_HTTP candidate=201 list=200 vote=201 duplicate=409 results=200/count1 realtimeAPIs=404 realtimeBeans=absent KafkaRedisConnections=0");
    }

    private HttpResponse<String> request(String method, String path, String body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10));
        if (method.equals("POST")) builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        else builder.GET();
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
}
