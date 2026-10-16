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
import org.example.livelunchchoice.vote.redis.VoteRankingRedis;
import org.example.livelunchchoice.vote.redis.VoteRankingPublisher;
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
    private final VoteRankingRedis rankingRedis;
    private final VoteRankingPublisher rankingPublisher;

    // 투표 등록: DB에 저장하고 Spring 이벤트를 발행한다. Kafka 발행은 커밋 후 리스너가 담당한다.
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

    // Redis 랭킹 조회: Sorted Set 순서를 그대로 반환하며 DB에 접근하지 않는다.
    public List<VoteResultResponse> rankings() {
        return rankingRedis.findAll();
    }

    // 이벤트 집계: Lua로 중복을 확인하고, 새로 집계한 이벤트만 Pub/Sub으로 알린다.
    public boolean aggregate(VoteCreateEvent event) {
        boolean aggregated = rankingRedis.incrementIfNotProcessed(event.eventId(), event.candidateId());
        if (aggregated) {
            rankingPublisher.publish(event.eventId());
        }
        return aggregated;
    }
}
