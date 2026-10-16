package org.example.livelunchchoice.vote.event;

import org.springframework.context.annotation.Profile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.livelunchchoice.vote.messaging.VoteEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("realtime")
public class VotedCreatedEventListener {
    private final VoteEventPublisher voteEventPublisher;

    @TransactionalEventListener(
            phase = TransactionPhase.AFTER_COMMIT
    )
    public void handle(VoteCreateEvent event){
        log.info(
                "VOTE_COMMITTED eventId={} voteId={}",
                event.eventId(),
                event.voteId()
        );

        voteEventPublisher.publish(event);
    }
}
