package org.example.livelunchchoice.vote.redis;

import org.springframework.context.annotation.Profile;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
@Profile("realtime")
public class VoteRankingRedisConfig {
    @Bean
    @ConditionalOnProperty(name = "app.ranking.subscription-enabled", havingValue = "true", matchIfMissing = true)
    public RedisMessageListenerContainer rankingListenerContainer(
            RedisConnectionFactory connectionFactory, VoteRankingSubscriber subscriber) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(subscriber, new ChannelTopic(VoteRankingPublisher.CHANNEL));
        return container;
    }
}
