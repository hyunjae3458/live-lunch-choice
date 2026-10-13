package org.example.livelunchchoice.vote.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class VoteKafkaConfig {
    @Bean
    public NewTopic voteCreatedTopic(){
        return TopicBuilder.name(VoteEventPublisher.TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
