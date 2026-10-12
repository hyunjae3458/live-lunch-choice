package org.example.livelunchchoice.vote.dto;

import java.time.Instant;

public record VoteResponse(long id, String userId, long candidateId, Instant createdAt) {
}
