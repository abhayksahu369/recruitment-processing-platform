package com.example.recruitment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * One recruiter-initiated bulk processing run: a job description plus an
 * uploaded candidate spreadsheet. Tracks how far processing has gotten via
 * plain counters rather than deriving progress from a COUNT query every time
 * someone polls {@code GET /api/processing/jobs/{id}} - Step 7's Kafka
 * consumers increment these directly as they finish each candidate.
 */
@Entity
@Table(name = "processing_jobs")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessingJob {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // Deterministic parse of the "Job Description" text block (Step 3).
    @Column(nullable = false)
    private String jobTitle;

    // Comma-separated, same raw shape as the source text ("Java, Spring Boot, SQL").
    // Split into a List<String> only where it's actually needed (the matcher),
    // rather than normalizing into a separate table for a field nothing else queries by.
    @Column(nullable = false)
    private String requiredSkills;

    @Column(nullable = false)
    private Integer minExperience;

    @Column(nullable = false)
    private Integer maxExperience;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProcessingStatus status;

    @Column(nullable = false)
    private int totalCandidates;

    @Column(nullable = false)
    private int processedCandidates;

    @Column(nullable = false)
    private int matchedCandidates;

    @Column(nullable = false)
    private int failedCandidates;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    private Instant completedAt;

    @PrePersist
    void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (status == null) {
            status = ProcessingStatus.QUEUED;
        }
    }
}
