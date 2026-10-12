package org.example.livelunchchoice.vote;

import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.candidate.CandidateRepository;
import org.example.livelunchchoice.candidate.exception.CandidateNotFoundException;
import org.example.livelunchchoice.common.error.UniqueConstraintViolation;
import org.example.livelunchchoice.vote.dto.VoteRequest;
import org.example.livelunchchoice.vote.dto.VoteResponse;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.example.livelunchchoice.vote.exception.AlreadyVotedException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class VoteService {
    private final VoteRepository voteRepository;
    private final CandidateRepository candidateRepository;

    @Transactional
    public VoteResponse create(VoteRequest request) {
        var candidate = candidateRepository.findById(request.candidateId())
                .orElseThrow(CandidateNotFoundException::new);
        if (voteRepository.existsByUserId(request.userId())) {
            throw new AlreadyVotedException();
        }
        try {
            var vote = voteRepository.saveAndFlush(new Vote(request.userId(), candidate));
            return new VoteResponse(vote.getId(), vote.getUserId(), candidate.getId(), vote.getCreatedAt());
        } catch (DataIntegrityViolationException exception) {
            // 실패한 트랜잭션에서 추가 조회·재시도 없이 예외를 던져 롤백한다.
            if (UniqueConstraintViolation.matches(exception, "uk_vote_user_id")) {
                throw new AlreadyVotedException();
            }
            throw exception;
        }
    }

    @Transactional(readOnly = true)
    public List<VoteResultResponse> results() {
        return voteRepository.findResults().stream()
                .map(result -> new VoteResultResponse(
                        result.getCandidateId(), result.getName(), result.getVoteCount()))
                .toList();
    }
}
