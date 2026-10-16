package org.example.livelunchchoice.common.config;

import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProcessingModeController {
    private final String mode;

    public ProcessingModeController(Environment environment) {
        // ProcessingModeConfig가 시작 시 정확히 한 모드만 허용한다.
        this.mode = environment.acceptsProfiles(Profiles.of("realtime")) ? "realtime" : "polling";
    }

    @GetMapping("/api/mode")
    public ResponseEntity<ModeResponse> mode() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(new ModeResponse(mode));
    }

    public record ModeResponse(String mode) { }
}
