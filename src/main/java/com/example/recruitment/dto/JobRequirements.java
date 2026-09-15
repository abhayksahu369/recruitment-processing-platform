package com.example.recruitment.dto;

import java.util.List;

/**
 * The deterministic-parsed result of a recruiter's job description text.
 * Consumed by the matching engine (Step 4) as the "what we're looking for"
 * side of a match.
 */
public record JobRequirements(
        String jobTitle,
        List<String> requiredSkills,
        int minExperience,
        int maxExperience
) {
}
