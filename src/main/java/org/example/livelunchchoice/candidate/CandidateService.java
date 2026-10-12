package org.example.livelunchchoice.candidate;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.candidate.dto.CandidateRequest;
import org.example.livelunchchoice.candidate.dto.CandidateResponse;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CandidateService {
    private final CandidateRepository candidateRepository;

    @Transactional
    public CandidateResponse create(CandidateRequest request){
        if(candidateRepository.existsByName(request.name())){
            throw new IllegalArgumentException("중복입니다.");
        }
        Candidate candidate = candidateRepository.save(new Candidate(request.name()));
        CandidateResponse response = new CandidateResponse(candidate.getId(), candidate.getName());
        return response;
    }
}
