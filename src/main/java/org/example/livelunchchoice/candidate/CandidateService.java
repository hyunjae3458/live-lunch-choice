package org.example.livelunchchoice.candidate;

import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.candidate.dto.CandidateRequest;
import org.example.livelunchchoice.candidate.dto.CandidateResponse;
import org.springframework.stereotype.Service;
import org.springframework.data.domain.Sort;

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
            throw new IllegalArgumentException("중복입니다.");
        }
        Candidate candidate = candidateRepository.save(new Candidate(request.name()));
        CandidateResponse response = new CandidateResponse(candidate.getId(), candidate.getName());
        return response;
    }
}
