package org.example.livelunchchoice.vote.event;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class VoteEventSerializationTests {
    @Test
    void mainYamlAllowsEventRoundTripWithTypeHeaders() {
        // 테스트용 YAML이 아닌 실제 앱 설정의 경로와 허용 패키지를 확인한다.
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new FileSystemResource("src/main/resources/application.yml"));
        Properties properties = yaml.getObject();
        assertThat(properties.getProperty("spring.kafka.producer.value-serializer"))
                .isEqualTo(JacksonJsonSerializer.class.getName());
        assertThat(properties.getProperty("spring.kafka.consumer.value-deserializer"))
                .isEqualTo(JacksonJsonDeserializer.class.getName());
        String trustedPackages = properties.getProperty(
                "spring.kafka.consumer.properties[spring.json.trusted.packages]");
        assertThat(trustedPackages).isNotBlank();

        VoteCreateEvent event = new VoteCreateEvent("event-1", 1L, 2L,
                Instant.parse("2026-10-13T10:00:00Z"));
        RecordHeaders headers = new RecordHeaders();
        try (JacksonJsonSerializer<VoteCreateEvent> serializer = new JacksonJsonSerializer<>();
             JacksonJsonDeserializer<VoteCreateEvent> deserializer = new JacksonJsonDeserializer<>()) {
            deserializer.configure(Map.of("spring.json.trusted.packages", trustedPackages), false);
            byte[] bytes = serializer.serialize("vote-created", headers, event);
            assertThat(deserializer.deserialize("vote-created", headers, bytes)).isEqualTo(event);
        }
    }
}
