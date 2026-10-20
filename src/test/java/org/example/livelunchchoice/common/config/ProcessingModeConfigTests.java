package org.example.livelunchchoice.common.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.assertj.core.api.Assertions.*;

class ProcessingModeConfigTests {
    @ParameterizedTest
    @ValueSource(strings = {"polling", "realtime", "local,polling", "local,realtime", "db-sse", "local,db-sse"})
    void acceptsExactlyOneMode(String profiles) {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles(profiles.split(","));
            context.register(ProcessingModeConfig.class);
            assertThatCode(context::refresh).doesNotThrowAnyException();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "local", "polling,realtime", "local,polling,realtime", "db-sse,realtime", "db-sse,polling", "db-sse,realtime,polling"})
    void rejectsMissingOrConflictingModes(String profiles) {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().setActiveProfiles(profiles.isEmpty() ? new String[0] : profiles.split(","));
            context.register(ProcessingModeConfig.class);
            assertThatThrownBy(context::refresh).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("정확히 하나");
        }
    }
}
