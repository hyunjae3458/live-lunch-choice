package org.example.livelunchchoice.common.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessingModeEnvironmentTests {
    @ParameterizedTest
    @CsvSource({"DEFAULT,realtime", "polling,polling", "realtime,realtime", "local;realtime,local;realtime"})
    void actualYamlSelectsModeWithRealtimeFallback(String setting, String expected) {
        SpringApplication application = new SpringApplication(ProcessingModeConfig.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setRegisterShutdownHook(false);
        StandardEnvironment environment = new StandardEnvironment();
        // 개발자의 실제 환경변수와 .env는 검증 결과에 영향을 주지 않는다.
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        application.setEnvironment(environment);
        List<String> arguments = new ArrayList<>(List.of(
                "--spring.config.location=file:src/main/resources/application.yml",
                "--spring.config.import=optional:classpath:/mode-test-no-env.properties",
                "--spring.main.banner-mode=off"));
        if (!setting.equals("DEFAULT")) arguments.add("--APP_PROFILE=" + setting.replace(';', ','));
        try (ConfigurableApplicationContext context = application.run(arguments.toArray(String[]::new))) {
            assertThat(context.getEnvironment().getActiveProfiles()).containsExactly(expected.split(";"));
        }
    }
}
