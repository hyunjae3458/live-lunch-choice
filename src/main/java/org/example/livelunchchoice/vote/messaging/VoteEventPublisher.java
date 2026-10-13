package org.example.livelunchchoice.vote.messaging;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class VoteEventPublisher {
    public static final String TOPIC = "vote-created";

    private final KafkaTemplate<String, VoteCreateEvent> kafkaTemplate;

    public void publish(VoteCreateEvent event){
        log.info(
                "PUBLISH_REQUESTED eventId={} voteId={}",
                event.eventId(),
                event.voteId()
        );

        try {
            kafkaTemplate.send(
                    TOPIC,
                    Long.toString(event.voteId()),
                    event
            ).whenComplete((result, exception) -> {
                if (exception != null) {
                    // 수신확인 실패
                    logFailure(event, exception);
                    return;
                }

                var metadata = result.getRecordMetadata();

                // 이벤트 kafka 수신확인 로그 출력
                log.info(
                        "PUBLISH_SUCCEEDED eventId={} topic={} partition={} offset={}",
                        event.eventId(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
            });
        } catch (Exception e) {
            // 전송 자체의 실패
            logFailure(event, e);
        }
    }

    // kafka 서버로의 이벤트 발송 실패 로그 출력
    private void logFailure(VoteCreateEvent event, Throwable exception){
        log.error(
                "PUBLISH_FAILED eventId={} voteId={}",
                event.eventId(),
                event.voteId(),
                exception
        );
    }
}
