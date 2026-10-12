package org.example.livelunchchoice.candidate;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.candidate.dto.CandidateRequest;
import org.example.livelunchchoice.candidate.dto.CandidateResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/candidates")
public class CandidateController {
    private final CandidateService candidateService;

    @GetMapping
    public ResponseEntity<List<CandidateResponse>> findAll() {
        return ResponseEntity.ok(candidateService.findAll());
    }

    @PostMapping
    public ResponseEntity<CandidateResponse> create(@Valid @RequestBody CandidateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(candidateService.create(request));
    }
}
