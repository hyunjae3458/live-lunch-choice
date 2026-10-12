package org.example.livelunchchoice.candidate;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.candidate.dto.CandidateRequest;
import org.example.livelunchchoice.candidate.dto.CandidateResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/candidates")
public class CandidateController {
    private final CandidateService candidateService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CandidateResponse create(@Valid @RequestBody CandidateRequest request) {
        return candidateService.create(request);
    }
}
