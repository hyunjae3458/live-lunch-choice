package org.example.livelunchchoice.vote.messaging.local;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Profile("local")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/local/kafka/consumer")
public class LocalConsumerController {
    private static final String LISTENER_ID = "vote-log-listener";
    private final KafkaListenerEndpointRegistry registry;

    @PostMapping("/stop")
    public synchronized ResponseEntity<Void> stop() {
        MessageListenerContainer container = container();
        if (container.isRunning()) {
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            container.stop(() -> stopped.complete(null));
            try {
                // isRunning=false만으로 종료 완료를 판단하지 않고 종료 콜백을 기다린다.
                stopped.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Consumer 종료 대기 중단", exception);
            } catch (ExecutionException | TimeoutException exception) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Consumer 종료 완료 미확인", exception);
            }
        }
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/start")
    public synchronized ResponseEntity<Void> start() {
        MessageListenerContainer container = container();
        if (!container.isRunning()) {
            container.start();
        }
        // 시작 요청 완료이며 파티션 할당이나 메시지 처리 완료를 뜻하지 않는다.
        return ResponseEntity.noContent().build();
    }

    @GetMapping
    public ResponseEntity<ConsumerStatus> status() {
        return ResponseEntity.ok(new ConsumerStatus(LISTENER_ID, container().isRunning()));
    }

    private MessageListenerContainer container() {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (container == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Consumer 컨테이너를 찾을 수 없습니다.");
        }
        return container;
    }

    public record ConsumerStatus(String listenerId, boolean running) {
    }
}
