package com.example.recruitment.dto;

import com.example.recruitment.domain.ProcessingStatus;

import java.util.UUID;

/** Response body for GET /api/processing/jobs/{jobId}. */
public record ProcessingJobStatusResponse(
        UUID jobId,
        ProcessingStatus status,
        int totalCandidates,
        int processed,
        int matched,
        int failed
) {
}
