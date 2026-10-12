package org.example.livelunchchoice.common.error;

import org.example.livelunchchoice.candidate.exception.CandidateNotFoundException;
import org.example.livelunchchoice.candidate.exception.DuplicateCandidateException;
import org.example.livelunchchoice.vote.exception.AlreadyVotedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(DuplicateCandidateException.class)
    public ResponseEntity<ErrorResponse> duplicateCandidate(DuplicateCandidateException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("DUPLICATE_CANDIDATE", exception.getMessage()));
    }

    @ExceptionHandler(CandidateNotFoundException.class)
    public ResponseEntity<ErrorResponse> candidateNotFound(CandidateNotFoundException exception) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse("CANDIDATE_NOT_FOUND", exception.getMessage()));
    }

    @ExceptionHandler(AlreadyVotedException.class)
    public ResponseEntity<ErrorResponse> alreadyVoted(AlreadyVotedException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse("ALREADY_VOTED", exception.getMessage()));
    }

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception exception, Object body,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        ErrorResponse error = status.is4xxClientError()
                ? new ErrorResponse("INVALID_REQUEST", "요청 형식이나 값을 확인해주세요.")
                : new ErrorResponse("INTERNAL_ERROR", "서버 오류가 발생했습니다.");
        return super.handleExceptionInternal(exception, error, headers, status, request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> unexpected(Exception exception) {
        log.error("처리되지 않은 서버 오류", exception);
        return ResponseEntity.internalServerError()
                .body(new ErrorResponse("INTERNAL_ERROR", "서버 오류가 발생했습니다."));
    }
}
