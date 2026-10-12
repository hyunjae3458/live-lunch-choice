package org.example.livelunchchoice.common.error;

import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ExceptionHandlingTests {
    @Test
    void identifiesOnlyKnownUniqueConstraints() {
        assertThat(UniqueConstraintViolation.matches(violation("23505", "uk_vote_user_id"),
                "uk_vote_user_id")).isTrue();
        assertThat(UniqueConstraintViolation.matches(violation("23505", "PUBLIC.UK_VOTE_USER_ID_INDEX_4"),
                "uk_vote_user_id")).isTrue();
        assertThat(UniqueConstraintViolation.matches(violation("23505", "uk_other"),
                "uk_vote_user_id")).isFalse();
        assertThat(UniqueConstraintViolation.matches(violation("23503", "fk_vote_candidate"),
                "uk_vote_user_id")).isFalse();
        assertThat(UniqueConstraintViolation.matches(violation("23502", "uk_vote_user_id"),
                "uk_vote_user_id")).isFalse();
        assertThat(UniqueConstraintViolation.matches(violation("23505", null),
                "uk_vote_user_id")).isFalse();
    }

    private DataIntegrityViolationException violation(String state, String constraint) {
        return new DataIntegrityViolationException("private SQL details",
                new ConstraintViolationException("private SQL details", new SQLException("private", state), constraint));
    }

    @Test
    void unknownIntegrityFailureIsNotDuplicateAndDoesNotLeakDetails() throws Exception {
        var mvc = MockMvcBuilders.standaloneSetup(new FailingController())
                .setControllerAdvice(new GlobalExceptionHandler()).build();
        mvc.perform(get("/test-failure"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().json("""
                        {"code":"INTERNAL_ERROR","message":"서버 오류가 발생했습니다."}
                        """))
                .andExpect(jsonPath("$.stackTrace").doesNotExist());
    }

    @RestController
    static class FailingController {
        @GetMapping("/test-failure")
        public String fail() {
            throw new DataIntegrityViolationException("private SQL details");
        }
    }
}
