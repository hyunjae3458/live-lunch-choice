package org.example.livelunchchoice.vote.messaging;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class VoteEventConsumer {
    @KafkaListener(
            id = "vote-log-listener",
            topics = VoteEventPublisher.TOPIC,
            groupId = "vote-log-group"
    )
    public void consume(ConsumerRecord<String, VoteCreateEvent> record){
        VoteCreateEvent event = record.value();

        // 컨슈머 소비 성공
        log.info(
                "CONSUMER_RECEIVED eventId={} voteId={} candidateId={} "
                        + "topic={} partition={} offset={}",
                event.eventId(),
                event.voteId(),
                event.candidateId(),
                record.topic(),
                record.partition(),
                record.offset()
        );
    }
}
