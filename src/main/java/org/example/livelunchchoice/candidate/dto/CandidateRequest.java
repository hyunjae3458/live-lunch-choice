package org.example.livelunchchoice.candidate.dto;

import jakarta.validation.constraints.NotBlank;

public record CandidateRequest(@NotBlank String name) {
}
