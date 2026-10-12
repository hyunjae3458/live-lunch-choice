package org.example.livelunchchoice.candidate.exception;

public class CandidateNotFoundException extends RuntimeException {
    public CandidateNotFoundException() {
        super("후보를 찾을 수 없습니다.");
    }
}
