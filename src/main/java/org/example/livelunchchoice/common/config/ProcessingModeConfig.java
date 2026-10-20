package org.example.livelunchchoice.common.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

@Configuration
public class ProcessingModeConfig {
    @Bean
    static BeanFactoryPostProcessor validateProcessingMode(Environment environment) {
        return beanFactory -> {
            boolean polling = environment.acceptsProfiles(Profiles.of("polling"));
            boolean realtime = environment.acceptsProfiles(Profiles.of("realtime"));
            boolean dbSse = environment.acceptsProfiles(Profiles.of("db-sse"));
            if ((polling ? 1 : 0) + (realtime ? 1 : 0) + (dbSse ? 1 : 0) != 1) {
                throw new IllegalStateException("polling, realtime, db-sse 프로필 중 정확히 하나를 활성화하세요.");
            }
        };
    }
}
