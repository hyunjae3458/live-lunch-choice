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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class CandidateControllerTests {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CandidateRepository candidateRepository;

    @BeforeEach
    void clearTestDatabase() {
        candidateRepository.deleteAll();
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
                .andExpect(status().isBadRequest());

        assertThat(candidateRepository.count()).isZero();
    }
}
