package org.example.livelunchchoice.vote;

import org.example.livelunchchoice.candidate.Candidate;
import org.example.livelunchchoice.candidate.CandidateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.example.livelunchchoice.vote.messaging.VoteEventPublisher;
import org.example.livelunchchoice.vote.dto.VoteRequest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@org.springframework.test.context.ActiveProfiles("realtime")
@SpringBootTest
@AutoConfigureMockMvc
class VoteApiTests {
    @Autowired
    private MockMvc mvc;
    @MockitoSpyBean
    private VoteRepository votes;
    @MockitoSpyBean
    private CandidateRepository candidates;

    @MockitoBean
    private VoteEventPublisher publisher;
    @Autowired
    private VoteService voteService;
    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void publishesOnlyAfterTransactionCommits() {
        Candidate candidate = candidates.save(new Candidate("한식"));
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        transaction.executeWithoutResult(status -> {
            voteService.create(new VoteRequest("commit-user", candidate.getId()));
            verifyNoInteractions(publisher);
        });

        ArgumentCaptor<VoteCreateEvent> event = ArgumentCaptor.forClass(VoteCreateEvent.class);
        verify(publisher).publish(event.capture());
        Vote saved = votes.findAll().get(0);
        assertThat(event.getValue().eventId()).isNotBlank();
        assertThat(event.getValue().voteId()).isEqualTo(saved.getId());
        assertThat(event.getValue().candidateId()).isEqualTo(candidate.getId());
    }

    @Test
    void rollbackDoesNotPublishEvent() {
        Candidate candidate = candidates.save(new Candidate("한식"));
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);
        transaction.executeWithoutResult(status -> {
            voteService.create(new VoteRequest("rollback-user", candidate.getId()));
            status.setRollbackOnly();
        });

        verifyNoInteractions(publisher);
        assertThat(votes.count()).isZero();
    }

    @BeforeEach
    void cleanDatabase() {
        votes.deleteAll();
        candidates.deleteAll();
    }

    private MvcResult submit(String userId, long candidateId) throws Exception {
        return mvc.perform(post("/api/votes").contentType(MediaType.APPLICATION_JSON)
                .content("{\"userId\":\"" + userId + "\",\"candidateId\":" + candidateId + "}"))
                .andReturn();
    }

    @Test
    void storesVoteAndReturnsPersistedInformation() throws Exception {
        var candidate = candidates.save(new Candidate("한식"));
        var result = submit("  user-1  ", candidate.getId());
        status().isCreated().match(result);
        var saved = votes.findAll();
        assertThat(saved).hasSize(1);
        assertThat(saved.get(0).getUserId()).isEqualTo("user-1");
        assertThat(saved.get(0).getCandidate().getId()).isEqualTo(candidate.getId());
        assertThat(saved.get(0).getCreatedAt()).isNotNull();
        jsonPath("$.id").value(saved.get(0).getId()).match(result);
        jsonPath("$.userId").value("user-1").match(result);
        jsonPath("$.candidateId").value(candidate.getId()).match(result);
        jsonPath("$.createdAt").isString().match(result);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{}", "{\"candidateId\":1}", "{\"userId\":null,\"candidateId\":1}",
            "{\"userId\":\"\",\"candidateId\":1}", "{\"userId\":\"   \",\"candidateId\":1}",
            "{\"userId\":\"u\"}", "{\"userId\":\"u\",\"candidateId\":null}",
            "{\"userId\":\"u\",\"candidateId\":0}", "{\"userId\":\"u\",\"candidateId\":-1}",
            "{\"userId\":\"u\",\"candidateId\":\"abc\"}"
    })
    void rejectsInvalidVoteWithoutSaving(String body) throws Exception {
        mvc.perform(post("/api/votes").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").isString());
        assertThat(votes.count()).isZero();
    }

    @Test
    void missingCandidateDoesNotSaveVote() throws Exception {
        var result = submit("user", Long.MAX_VALUE);
        status().isNotFound().match(result);
        jsonPath("$.code").value("CANDIDATE_NOT_FOUND").match(result);
        assertThat(votes.count()).isZero();
    }

    @Test
    void rejectsValuesExceedingDatabaseColumnLength() throws Exception {
        String tooLong = "a".repeat(256);
        mvc.perform(post("/api/candidates").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"" + tooLong + "\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
        assertThat(candidates.count()).isZero();

        var candidate = candidates.save(new Candidate("한식"));
        var result = submit(tooLong, candidate.getId());
        status().isBadRequest().match(result);
        jsonPath("$.code").value("INVALID_REQUEST").match(result);
        assertThat(votes.count()).isZero();
    }

    @Test
    void acceptsMaximumLengthAfterStrippingWhitespace() throws Exception {
        String maximum = "a".repeat(255);
        mvc.perform(post("/api/candidates").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\" " + maximum + " \"}"))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.name").value(maximum));
        var result = submit(" " + maximum + " ", candidates.findAll().get(0).getId());
        status().isCreated().match(result);
        jsonPath("$.userId").value(maximum).match(result);
    }

    @Test
    void rejectsSecondVoteEvenForDifferentCandidate() throws Exception {
        var first = candidates.save(new Candidate("한식"));
        var second = candidates.save(new Candidate("중식"));
        status().isCreated().match(submit("user", first.getId()));
        var result = submit(" user ", second.getId());
        status().isConflict().match(result);
        jsonPath("$.code").value("ALREADY_VOTED").match(result);
        assertThat(votes.count()).isEqualTo(1);
        assertThat(votes.findAll().get(0).getCandidate().getId()).isEqualTo(first.getId());
    }

    @Test
    void concurrentVotesPassPrecheckButOnlyOneCommits() throws Exception {
        var first = candidates.save(new Candidate("한식"));
        var second = candidates.save(new Candidate("중식"));
        var barrier = new CyclicBarrier(2);
        var delegate = mockingDetails(votes).getMockCreationSettings().getDefaultAnswer();
        doAnswer(invocation -> {
            Object exists = delegate.answer(invocation);
            assertThat(exists).isEqualTo(false);
            barrier.await(10, TimeUnit.SECONDS);
            return exists;
        }).when(votes).existsByUserId("racer");

        var pool = Executors.newFixedThreadPool(2);
        try {
            var a = pool.submit(() -> submit("racer", first.getId()));
            var b = pool.submit(() -> submit(" racer ", second.getId()));
            var results = List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
            assertThat(results).extracting(r -> r.getResponse().getStatus())
                    .containsExactlyInAnyOrder(201, 409);
            for (var result : results) {
                if (result.getResponse().getStatus() == 409) {
                    jsonPath("$.code").value("ALREADY_VOTED").match(result);
                }
            }
            assertThat(votes.count()).isEqualTo(1);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void normalizesCandidateNameAndRejectsDuplicate() throws Exception {
        mvc.perform(post("/api/candidates").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"  한식 정식  \"}"))
                .andExpect(status().isCreated()).andExpect(jsonPath("$.name").value("한식 정식"));
        mvc.perform(post("/api/candidates").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"한식 정식\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("DUPLICATE_CANDIDATE"));
        assertThat(candidates.findAll()).extracting(Candidate::getName).containsExactly("한식 정식");
    }

    @Test
    void concurrentCandidateRegistrationReturnsConflict() throws Exception {
        var barrier = new CyclicBarrier(2);
        var delegate = mockingDetails(candidates).getMockCreationSettings().getDefaultAnswer();
        doAnswer(invocation -> {
            Object exists = delegate.answer(invocation);
            assertThat(exists).isEqualTo(false);
            barrier.await(10, TimeUnit.SECONDS);
            return exists;
        }).when(candidates).existsByName("한식");
        var pool = Executors.newFixedThreadPool(2);
        try {
            java.util.concurrent.Callable<MvcResult> request = () -> mvc.perform(post("/api/candidates")
                    .contentType(MediaType.APPLICATION_JSON).content("{\"name\":\" 한식 \"}"))
                    .andReturn();
            var a = pool.submit(request);
            var b = pool.submit(request);
            var results = List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
            assertThat(results).extracting(r -> r.getResponse().getStatus())
                    .containsExactlyInAnyOrder(201, 409);
            for (var result : results) {
                if (result.getResponse().getStatus() == 409) {
                    jsonPath("$.code").value("DUPLICATE_CANDIDATE").match(result);
                }
            }
            assertThat(candidates.count()).isEqualTo(1);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void aggregatesCountsIncludesZeroAndSortsTiesById() throws Exception {
        var a = candidates.save(new Candidate("한식"));
        var b = candidates.save(new Candidate("중식"));
        var c = candidates.save(new Candidate("일식"));
        var zero = candidates.save(new Candidate("양식"));
        status().isCreated().match(submit("u1", b.getId()));
        status().isCreated().match(submit("u2", a.getId()));
        status().isCreated().match(submit("u3", b.getId()));
        status().isCreated().match(submit("u4", c.getId()));
        mvc.perform(get("/api/votes/results"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(4))
                .andExpect(jsonPath("$[0].candidateId").value(b.getId()))
                .andExpect(jsonPath("$[0].name").value("중식"))
                .andExpect(jsonPath("$[0].voteCount").value(2))
                .andExpect(jsonPath("$[1].candidateId").value(a.getId()))
                .andExpect(jsonPath("$[1].voteCount").value(1))
                .andExpect(jsonPath("$[2].candidateId").value(c.getId()))
                .andExpect(jsonPath("$[2].voteCount").value(1))
                .andExpect(jsonPath("$[3].candidateId").value(zero.getId()))
                .andExpect(jsonPath("$[3].voteCount").value(0));
    }

    @Test
    void returnsEmptyResultsWithoutCandidates() throws Exception {
        mvc.perform(get("/api/votes/results"))
                .andExpect(status().isOk()).andExpect(content().json("[]"));
    }

    @Test
    void returnsAllCandidatesWithZeroVotes() throws Exception {
        var a = candidates.save(new Candidate("한식"));
        var b = candidates.save(new Candidate("중식"));
        mvc.perform(get("/api/votes/results"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].candidateId").value(a.getId()))
                .andExpect(jsonPath("$[0].voteCount").value(0))
                .andExpect(jsonPath("$[1].candidateId").value(b.getId()))
                .andExpect(jsonPath("$[1].voteCount").value(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/votes", "/api/candidates"})
    void malformedJsonUsesCommonResponse(String endpoint) throws Exception {
        mvc.perform(post(endpoint).contentType(MediaType.APPLICATION_JSON).content("{broken"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").value("요청 형식이나 값을 확인해주세요."));
    }

    @Test
    void preservesMethodNotAllowedStatusAndAllowHeader() throws Exception {
        mvc.perform(delete("/api/votes"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(header().exists("Allow"))
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));
    }
}
