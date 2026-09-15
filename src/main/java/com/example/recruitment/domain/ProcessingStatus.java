package com.example.recruitment.domain;

/**
 * Lifecycle of a {@link ProcessingJob}. A job starts QUEUED, moves to
 * PROCESSING once Kafka consumers start picking up its candidates, and ends
 * at either COMPLETED or FAILED. See {@link ProcessingJob} for how the
 * counters that drive these transitions are tracked.
 */
public enum ProcessingStatus {
    QUEUED,
    PROCESSING,
    COMPLETED,
    FAILED
}
