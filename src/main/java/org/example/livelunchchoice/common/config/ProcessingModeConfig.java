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
            if (polling == realtime) {
                throw new IllegalStateException("polling 또는 realtime 프로필 중 정확히 하나를 활성화하세요.");
            }
        };
    }
}
