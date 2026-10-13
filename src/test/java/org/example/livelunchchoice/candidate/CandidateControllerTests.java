package org.example.livelunchchoice.candidate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.example.livelunchchoice.vote.VoteRepository;
import org.example.livelunchchoice.vote.messaging.VoteEventPublisher;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@MockitoBean(types = VoteEventPublisher.class)
@AutoConfigureMockMvc
class CandidateControllerTests {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CandidateRepository candidateRepository;

    @Autowired
    private VoteRepository voteRepository;

    @BeforeEach
    void clearTestDatabase() {
        voteRepository.deleteAll();
        candidateRepository.deleteAll();
    }

    @Test
    void returnsEmptyListWhenNoCandidatesExist() throws Exception {
        mockMvc.perform(get("/api/candidates"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$").isEmpty());
    }

    @Test
    void returnsAllCandidatesInIdOrder() throws Exception {
        var first = candidateRepository.save(new Candidate("한식"));
        var second = candidateRepository.save(new Candidate("중식"));

        mockMvc.perform(get("/api/candidates"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].id").value(first.getId()))
                .andExpect(jsonPath("$[0].name").value("한식"))
                .andExpect(jsonPath("$[1].id").value(second.getId()))
                .andExpect(jsonPath("$[1].name").value("중식"));
    }

    @Test
    void createsCandidateAndReturnsStoredIdAndName() throws Exception {
        mockMvc.perform(post("/api/candidates")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"한식\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNumber())
                .andExpect(jsonPath("$.name").value("한식"))
                .andExpect(result -> {
                    var candidates = candidateRepository.findAll();
                    assertThat(candidates).hasSize(1);
                    var saved = candidates.get(0);
                    assertThat(saved.getId()).isPositive();
                    assertThat(saved.getName()).isEqualTo("한식");
                    jsonPath("$.id").value(saved.getId()).match(result);
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"name\":null}", "{\"name\":\"\"}", "{\"name\":\"   \"}"})
    void rejectsInvalidNameWithoutSaving(String requestBody) throws Exception {
        mockMvc.perform(post("/api/candidates")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.message").isString());

        assertThat(candidateRepository.count()).isZero();
    }
}
