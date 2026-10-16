package org.example.livelunchchoice.vote;

import org.example.livelunchchoice.candidate.CandidateRepository;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.example.livelunchchoice.vote.redis.VoteRankingRedis;
import org.example.livelunchchoice.vote.redis.VoteRankingPublisher;
import org.example.livelunchchoice.vote.sse.VoteRankingSseService;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class VoteRankingControllerTests {
    private final VoteRankingRedis redis = mock(VoteRankingRedis.class);
    private final VoteService service = new VoteService(mock(VoteRepository.class),
            mock(CandidateRepository.class), mock(ApplicationEventPublisher.class), redis, mock(VoteRankingPublisher.class));
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(
            new VoteRankingController(service, new VoteRankingSseService(service))).build();

    @Test
    void rankingsPreserveOrderAndOnlyExposeIdAndCount() throws Exception {
        when(redis.findAll()).thenReturn(List.of(new VoteResultResponse(2, 3), new VoteResultResponse(10, 3)));
        mvc.perform(get("/api/votes/rankings"))
                .andExpect(status().isOk()).andExpect(content().json("[{\"candidateId\":2,\"voteCount\":3},{\"candidateId\":10,\"voteCount\":3}]"))
                .andExpect(jsonPath("$[0].candidateId").value(2)).andExpect(jsonPath("$[0].name").doesNotExist());
    }

    @Test
    void emptyRankingsReturnEmptyArray() throws Exception {
        when(redis.findAll()).thenReturn(List.of());
        mvc.perform(get("/api/votes/rankings")).andExpect(status().isOk()).andExpect(content().json("[]"));
    }

    @Test
    void streamStartsWithNamedJsonSnapshot() throws Exception {
        when(redis.findAll()).thenReturn(List.of(new VoteResultResponse(2, 3)));
        mvc.perform(get("/api/votes/rankings/stream"))
                .andExpect(request().asyncStarted()).andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("text/event-stream"))
                .andExpect(content().string("event:ranking\ndata:[{\"candidateId\":2,\"voteCount\":3}]\n\n"));
    }
}
