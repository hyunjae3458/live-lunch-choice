package org.example.livelunchchoice.candidate.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CandidateRequest(@NotBlank @Size(max = 255) String name) {
    public CandidateRequest {
        if (name != null) {
            name = name.strip();
        }
    }
}
