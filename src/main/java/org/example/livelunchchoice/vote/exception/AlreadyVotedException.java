package org.example.livelunchchoice.vote.exception;

public class AlreadyVotedException extends RuntimeException {
    public AlreadyVotedException() {
        super("이미 투표한 사용자입니다.");
    }
}
