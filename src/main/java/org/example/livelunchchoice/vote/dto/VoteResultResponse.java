package org.example.livelunchchoice.vote.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

public record VoteResultResponse(long candidateId, @JsonInclude(JsonInclude.Include.NON_NULL) String name,
                                 long voteCount) {
    public VoteResultResponse(long candidateId, long voteCount) {
        this(candidateId, null, voteCount);
    }
}
