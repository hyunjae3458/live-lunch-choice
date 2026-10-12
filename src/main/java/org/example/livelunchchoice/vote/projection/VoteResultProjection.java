package org.example.livelunchchoice.vote.projection;

public interface VoteResultProjection {
    Long getCandidateId();

    String getName();

    Long getVoteCount();
}
