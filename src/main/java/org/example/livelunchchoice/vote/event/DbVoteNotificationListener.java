package org.example.livelunchchoice.vote.event;

import jakarta.annotation.PreDestroy;
import org.example.livelunchchoice.vote.sse.VoteRankingSseService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.LoggerFactory;

@Component
@Profile("db-sse")
public class DbVoteNotificationListener {
    private final VoteRankingSseService sse;
    private final ThreadPoolExecutor executor;

    public DbVoteNotificationListener(VoteRankingSseService sse,
            @Value("${app.db-sse.workers:1}") int workers,
            @Value("${app.db-sse.queue-capacity:1024}") int capacity) {
        this.sse = sse;
        this.executor = new ThreadPoolExecutor(workers, workers, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity), new ThreadPoolExecutor.AbortPolicy());
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(VoteCreateEvent event) {
        try {
            executor.execute(() -> {
                try {
                    sse.broadcast();
                } catch (RuntimeException exception) {
                    LoggerFactory.getLogger(getClass()).error("DB SSE notification failed: {}", event.eventId(), exception);
                }
            });
        } catch (RejectedExecutionException exception) {
            // DB는 이미 커밋됐다. 저장 성공과 알림 실패를 구분하며 요청 스레드에서 대신 실행하지 않는다.
            LoggerFactory.getLogger(getClass()).error("DB SSE queue full: {}", event.eventId());
        }
    }

    public int queueDepth() { return executor.getQueue().size(); }
    @PreDestroy
    public void close() { executor.shutdown(); }
}
