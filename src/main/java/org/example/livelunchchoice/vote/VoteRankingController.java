package org.example.livelunchchoice.vote;

import lombok.RequiredArgsConstructor;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;
import org.example.livelunchchoice.vote.sse.VoteRankingSseService;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/votes/rankings")
public class VoteRankingController {
    private final VoteService voteService;
    private final VoteRankingSseService sseService;

    @GetMapping
    public ResponseEntity<List<VoteResultResponse>> rankings() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(voteService.rankings());
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> stream() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(sseService.subscribe());
    }
}
