package org.example.livelunchchoice.vote.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record VoteRequest(@NotBlank @Size(max = 255) String userId,
                          @NotNull @Positive Long candidateId) {
    public VoteRequest {
        if (userId != null) {
            userId = userId.strip();
        }
    }
}
