package org.example.livelunchchoice.vote;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.example.livelunchchoice.candidate.Candidate;
import org.example.livelunchchoice.candidate.CandidateRepository;
import org.example.livelunchchoice.vote.dto.VoteRequest;
import org.example.livelunchchoice.vote.event.DbVoteNotificationListener;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.example.livelunchchoice.vote.sse.VoteRankingSseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("db-sse")
class DbSseProfileTests {
    @Autowired VoteService votes;
    @Autowired CandidateRepository candidates;
    @Autowired VoteRepository repository;
    @Autowired RankingSource rankings;
    @Autowired PlatformTransactionManager transactionManager;
    @MockitoSpyBean VoteRankingSseService sse;

    @Test
    void committedVoteIsVisibleAndRollbackDoesNotNotify() throws Exception {
        Candidate candidate = candidates.saveAndFlush(new Candidate("db-sse-test"));
        CountDownLatch delivered = new CountDownLatch(1);
        doAnswer(invocation -> {
            invocation.callRealMethod();
            assertThat(rankings.rankings()).anySatisfy(row -> {
                assertThat(row.candidateId()).isEqualTo(candidate.getId());
                assertThat(row.voteCount()).isEqualTo(1);
                assertThat(row.name()).isNull();
            });
            delivered.countDown();
            return null;
        }).when(sse).broadcast();
        votes.create(new VoteRequest("db-sse-committed", candidate.getId()));
        assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        verify(sse, times(1)).broadcast();
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            votes.create(new VoteRequest("db-sse-rollback", candidate.getId()));
            status.setRollbackOnly();
        });
        assertThat(repository.existsByUserId("db-sse-rollback")).isFalse();
        verify(sse, times(1)).broadcast();
    }

    @Test
    void boundedQueueRejectsWithoutExecutingOnRequestThread() throws Exception {
        VoteRankingSseService blocked = mock(VoteRankingSseService.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> { entered.countDown(); release.await(5, TimeUnit.SECONDS); return null; })
                .when(blocked).broadcast();
        DbVoteNotificationListener listener = new DbVoteNotificationListener(blocked, 1, 1);
        try {
            listener.handle(new VoteCreateEvent("one", 1, 1, java.time.Instant.now()));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            listener.handle(new VoteCreateEvent("two", 2, 1, java.time.Instant.now()));
            assertThatCode(() -> listener.handle(new VoteCreateEvent("rejected", 3, 1, java.time.Instant.now())))
                    .doesNotThrowAnyException();
            assertThat(listener.queueDepth()).isEqualTo(1);
            verify(blocked, times(1)).broadcast();
        } finally { release.countDown(); listener.close(); }
    }
}
