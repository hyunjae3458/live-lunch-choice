package org.example.livelunchchoice.candidate;

import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.candidate.dto.CandidateRequest;
import org.example.livelunchchoice.candidate.dto.CandidateResponse;
import org.springframework.stereotype.Service;
import org.springframework.data.domain.Sort;
import org.springframework.dao.DataIntegrityViolationException;
import org.example.livelunchchoice.candidate.exception.DuplicateCandidateException;
import org.example.livelunchchoice.common.error.UniqueConstraintViolation;

import java.util.List;

@Service
@RequiredArgsConstructor
public class CandidateService {
    private final CandidateRepository candidateRepository;

    @Transactional(readOnly = true)
    public List<CandidateResponse> findAll() {
        return candidateRepository.findAll(Sort.by("id").ascending()).stream()
                .map(candidate -> new CandidateResponse(candidate.getId(), candidate.getName()))
                .toList();
    }

    @Transactional
    public CandidateResponse create(CandidateRequest request){
        if(candidateRepository.existsByName(request.name())){
            throw new DuplicateCandidateException();
        }
        try {
            Candidate candidate = candidateRepository.saveAndFlush(new Candidate(request.name()));
            return new CandidateResponse(candidate.getId(), candidate.getName());
        } catch (DataIntegrityViolationException exception) {
            if (UniqueConstraintViolation.matches(exception, "uk_candidate_name")) {
                throw new DuplicateCandidateException();
            }
            throw exception;
        }
    }
}
