package org.example.livelunchchoice.vote.sse;

import org.springframework.context.annotation.Profile;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.vote.RankingSource;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Profile({"realtime", "db-sse"})
public class VoteRankingSseService {
    private final RankingSource rankingSource;
    private final Set<SseEmitter> emitters = ConcurrentHashMap.newKeySet();

    // 조회부터 전송까지 같은 잠금으로 직렬화하여 오래된 초기 응답이 변경 응답 뒤에 가지 않게 한다.
    public synchronized SseEmitter subscribe() {
        SseEmitter emitter = createEmitter();
        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> {
            emitters.remove(emitter);
            emitter.complete();
        });
        emitter.onError(error -> emitters.remove(emitter));
        emitters.add(emitter);
        try {
            send(emitter, rankingSource.rankings());
            return emitter;
        } catch (RuntimeException exception) {
            emitters.remove(emitter);
            emitter.completeWithError(exception);
            throw exception;
        }
    }

    public synchronized void broadcast() {
        List<VoteResultResponse> rankings = rankingSource.rankings();
        for (SseEmitter emitter : emitters) {
            send(emitter, rankings);
        }
    }

    private void send(SseEmitter emitter, List<VoteResultResponse> rankings) {
        try {
            emitter.send(SseEmitter.event().name("ranking").data(rankings, MediaType.APPLICATION_JSON));
        } catch (IOException exception) {
            // 전송 I/O 오류의 응답 종료는 Servlet/Spring이 처리한다.
            emitters.remove(emitter);
        } catch (IllegalStateException exception) {
            emitters.remove(emitter);
            emitter.completeWithError(exception);
        }
    }

    SseEmitter createEmitter() {
        return new SseEmitter(60_000L);
    }
}
