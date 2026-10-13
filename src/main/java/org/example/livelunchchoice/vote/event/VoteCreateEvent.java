package org.example.livelunchchoice.vote.event;

import java.time.Instant;

public record VoteCreateEvent(String eventId, long voteId, long candidateId, Instant occurredAt) {
}
