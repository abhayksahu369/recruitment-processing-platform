package com.example.recruitment.dto;

import com.example.recruitment.domain.ProcessingStatus;

import java.util.UUID;

/**
 * Response body for GET /api/processing/jobs/{jobId}.
 * <p>
 * processingDurationMillis and recordsPerSecond are computed at read time
 * from createdAt/completedAt and the counters above - nothing here is
 * stored. While a job is still running, "duration" is elapsed-so-far
 * (createdAt to now) and "records per second" is a live, continuously
 * updating throughput figure, not just a final summary.
 */
public record ProcessingJobStatusResponse(
        UUID jobId,
        ProcessingStatus status,
        int totalCandidates,
        int processed,
        int matched,
        int failed,
        long processingDurationMillis,
        double recordsPerSecond
) {
}
