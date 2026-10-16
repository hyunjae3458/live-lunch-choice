package org.example.livelunchchoice.vote.messaging;

import org.springframework.context.annotation.Profile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.livelunchchoice.vote.VoteRealtimeService;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("realtime")
public class VoteEventConsumer {
    private final VoteRealtimeService realtimeService;

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

        boolean aggregated = realtimeService.aggregate(event);

        if(!aggregated){
            log.info(
                    "VOTE_DUPLICATE_SKIPPED eventId={} voteId={} candidateId={}",
                    event.eventId(),
                    event.voteId(),
                    event.candidateId()
            );
            return;
        }

        log.info(
                "VOTE_AGGREGATED eventId={} voteId={} candidateId={}",
                event.eventId(),
                event.voteId(),
                event.candidateId()
        );
    }
}
