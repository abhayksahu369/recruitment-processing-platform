package com.example.recruitment.dto;

import com.example.recruitment.domain.ProcessingStatus;

import java.util.UUID;

/** Response body for POST /api/processing/jobs. */
public record CreateProcessingJobResponse(UUID jobId, ProcessingStatus status, int totalCandidates) {
}
