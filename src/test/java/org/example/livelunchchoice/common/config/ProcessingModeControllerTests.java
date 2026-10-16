package org.example.livelunchchoice.common.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ProcessingModeControllerTests {
    @ParameterizedTest
    @CsvSource({"polling,polling", "realtime,realtime", "local;polling,polling", "local;realtime,realtime"})
    void exposesOnlyModeWithoutProfilesOrSecrets(String profiles, String expected) throws Exception {
        MockEnvironment environment = new MockEnvironment();
        environment.setActiveProfiles(profiles.split(";"));
        environment.setProperty("DB_PASSWORD", "must-not-be-exposed");
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new ProcessingModeController(environment)).build();
        mvc.perform(get("/api/mode"))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(content().string("{\"mode\":\"" + expected + "\"}"));
    }
}
