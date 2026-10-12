package org.example.livelunchchoice.candidate.exception;

public class DuplicateCandidateException extends RuntimeException {
    public DuplicateCandidateException() {
        super("이미 등록된 후보입니다.");
    }
}
