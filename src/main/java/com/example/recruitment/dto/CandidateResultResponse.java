package com.example.recruitment.dto;

import java.util.List;

/** One entry in GET /api/processing/jobs/{jobId}/results, ranked by finalScore descending. */
public record CandidateResultResponse(
        String candidateId,
        String name,
        double finalScore,
        List<String> matchedSkills,
        List<String> missingSkills
) {
}
