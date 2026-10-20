package org.example.livelunchchoice.vote;

import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.candidate.Candidate;
import org.example.livelunchchoice.candidate.CandidateRepository;
import org.example.livelunchchoice.candidate.exception.CandidateNotFoundException;
import org.example.livelunchchoice.common.error.UniqueConstraintViolation;
import org.example.livelunchchoice.vote.event.VoteCreateEvent;
import org.example.livelunchchoice.vote.dto.VoteRequest;
import org.example.livelunchchoice.vote.dto.VoteResponse;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.example.livelunchchoice.vote.exception.AlreadyVotedException;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class VoteService {
    private final VoteRepository voteRepository;
    private final CandidateRepository candidateRepository;
    private final ApplicationEventPublisher applicationEventPublisher;

    // 투표 등록: DB 저장 후 Spring 이벤트 발행. 모드별 커밋 후 리스너가 알림 처리를 맡는다.
    @Transactional
    public VoteResponse create(VoteRequest request) {
        Candidate candidate = candidateRepository.findById(request.candidateId())
                .orElseThrow(CandidateNotFoundException::new);
        if (voteRepository.existsByUserId(request.userId())) {
            throw new AlreadyVotedException();
        }

        Vote vote;

        try {
            vote = voteRepository.saveAndFlush(new Vote(request.userId(), candidate));
        } catch (DataIntegrityViolationException exception) {
            // 실패한 트랜잭션에서 추가 조회·재시도 없이 예외를 던져 롤백한다.
            if (UniqueConstraintViolation.matches(exception, "uk_vote_user_id")) {
                throw new AlreadyVotedException();
            }
            throw exception;
        }

        // 투표 이벤트 발행
        VoteCreateEvent event = new VoteCreateEvent(
                UUID.randomUUID().toString(),
                vote.getId(),
                candidate.getId(),
                vote.getCreatedAt()
        );
        applicationEventPublisher.publishEvent(event);

        return new VoteResponse(
                vote.getId(),
                vote.getUserId(),
                candidate.getId(),
                vote.getCreatedAt()
        );
    }

    // DB 집계 조회: 후보 이름과 0표 후보를 포함한 결과를 반환한다.
    @Transactional(readOnly = true)
    public List<VoteResultResponse> results() {
        return voteRepository.findResults().stream()
                .map(result -> new VoteResultResponse(
                        result.getCandidateId(), result.getName(), result.getVoteCount()))
                .toList();
    }

}
