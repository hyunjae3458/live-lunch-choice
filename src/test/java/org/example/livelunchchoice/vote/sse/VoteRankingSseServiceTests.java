package org.example.livelunchchoice.vote.sse;

import org.example.livelunchchoice.vote.VoteService;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class VoteRankingSseServiceTests {
    private final VoteService rankings = mock(VoteService.class);
    private final VoteRankingSseService service = spy(new VoteRankingSseService(rankings));

    @Test
    void sendsInitialThenBroadcastToBothConnectionsAndRemovesCompletedConnection() throws Exception {
        SseEmitter first = mock(SseEmitter.class);
        SseEmitter second = mock(SseEmitter.class);
        doReturn(first, second).when(service).createEmitter();
        when(rankings.rankings()).thenReturn(List.of());
        service.subscribe();
        service.subscribe();
        service.broadcast();
        verify(first, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        verify(second, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        ArgumentCaptor<Runnable> completion = ArgumentCaptor.forClass(Runnable.class);
        verify(first).onCompletion(completion.capture());
        completion.getValue().run();
        service.broadcast();
        verify(first, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        verify(second, times(3)).send(any(SseEmitter.SseEventBuilder.class));
        verify(rankings, times(4)).rankings(); // each subscription + once per broadcast
    }

    @Test
    @SuppressWarnings("unchecked")
    void timeoutAndErrorCallbacksRemoveConnections() throws Exception {
        SseEmitter first = mock(SseEmitter.class);
        SseEmitter second = mock(SseEmitter.class);
        doReturn(first, second).when(service).createEmitter();
        when(rankings.rankings()).thenReturn(List.of());
        service.subscribe(); service.subscribe();
        ArgumentCaptor<Runnable> timeout = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Consumer<Throwable>> error = ArgumentCaptor.forClass(Consumer.class);
        verify(first).onTimeout(timeout.capture());
        verify(second).onError(error.capture());
        timeout.getValue().run(); error.getValue().accept(new IOException("closed"));
        service.broadcast();
        verify(first).complete();
        verify(first, times(1)).send(any(SseEmitter.SseEventBuilder.class));
        verify(second, times(1)).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void failedSendIsRemovedAndDoesNotPreventOtherConnections() throws Exception {
        SseEmitter failed = mock(SseEmitter.class);
        SseEmitter healthy = mock(SseEmitter.class);
        doReturn(failed, healthy).when(service).createEmitter();
        when(rankings.rankings()).thenReturn(List.of());
        service.subscribe(); service.subscribe();
        doThrow(new IOException("closed")).when(failed).send(any(SseEmitter.SseEventBuilder.class));
        service.broadcast(); service.broadcast();
        verify(failed, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        verify(healthy, times(3)).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void initialLookupFailureDoesNotLeakConnection() throws Exception {
        SseEmitter emitter = mock(SseEmitter.class);
        doReturn(emitter).when(service).createEmitter();
        RuntimeException failure = new IllegalStateException("lookup failed");
        when(rankings.rankings()).thenThrow(failure).thenReturn(List.of());
        assertThatThrownBy(service::subscribe).isSameAs(failure);
        service.broadcast();
        verify(emitter).completeWithError(failure);
        verify(emitter, never()).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void initialSnapshotCannotBeOvertakenByBroadcast() throws Exception {
        SseEmitter emitter = mock(SseEmitter.class);
        doReturn(emitter).when(service).createEmitter();
        CountDownLatch lookupEntered = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        CountDownLatch broadcastStarted = new CountDownLatch(1);
        List<VoteResultResponse> initial = List.of(new VoteResultResponse(1, 1));
        List<VoteResultResponse> updated = List.of(new VoteResultResponse(1, 2));
        when(rankings.rankings()).thenAnswer(invocation -> {
            lookupEntered.countDown();
            assertThat(releaseLookup.await(5, TimeUnit.SECONDS)).isTrue();
            return initial;
        }).thenReturn(updated);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> subscription = executor.submit(service::subscribe);
            assertThat(lookupEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<?> broadcast = executor.submit(() -> { broadcastStarted.countDown(); service.broadcast(); });
            assertThat(broadcastStarted.await(5, TimeUnit.SECONDS)).isTrue();
            releaseLookup.countDown();
            subscription.get(5, TimeUnit.SECONDS); broadcast.get(5, TimeUnit.SECONDS);
            ArgumentCaptor<SseEmitter.SseEventBuilder> sends = ArgumentCaptor.forClass(SseEmitter.SseEventBuilder.class);
            verify(emitter, times(2)).send(sends.capture());
            assertThat(sends.getAllValues().get(0).build()).anySatisfy(data -> assertThat(data.getData()).isEqualTo(initial));
            assertThat(sends.getAllValues().get(1).build()).anySatisfy(data -> assertThat(data.getData()).isEqualTo(updated));
        } finally {
            releaseLookup.countDown(); executor.shutdownNow();
        }
    }
}
