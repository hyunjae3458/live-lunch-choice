package org.example.livelunchchoice.vote;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.vote.dto.VoteRequest;
import org.example.livelunchchoice.vote.dto.VoteResponse;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/votes")
public class VoteController {
    private final VoteService voteService;

    @PostMapping
    public ResponseEntity<VoteResponse> create(@Valid @RequestBody VoteRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(voteService.create(request));
    }

    @GetMapping("/results")
    public ResponseEntity<List<VoteResultResponse>> results() {
        return ResponseEntity.ok(voteService.results());
    }
}
