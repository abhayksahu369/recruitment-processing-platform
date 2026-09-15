package com.example.recruitment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
 * The output of running a {@code CandidateMatcher} on one candidate against
 * one job's requirements. Deliberately references {@code processingJobId}
 * and {@code candidateId} as plain UUID columns rather than JPA
 * {@code @ManyToOne} relationships - the same "send IDs, not objects"
 * principle used for Kafka messages (see the kafka package). A Kafka
 * consumer writing thousands of these under load should never risk
 * triggering a lazy-load or cascade it didn't ask for; anything that needs
 * candidate details (name, email) looks them up explicitly via
 * {@code CandidateRepository} instead of navigating an association here.
 *
 * skillScore and experienceScore are stored as 0.0-1.0 ratios, matching the
 * matching-engine formulas exactly (Step 4). Percentage formatting (e.g.
 * 0.92 -> "92.0") is a presentation concern that belongs at the API/DTO
 * boundary, not in this table.
 */
@Entity
@Table(name = "match_results")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MatchResult {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "processing_job_id", nullable = false)
    private UUID processingJobId;

    @Column(name = "candidate_id", nullable = false)
    private UUID candidateId;

    @Column(nullable = false)
    private Double skillScore;

    @Column(nullable = false)
    private Double experienceScore;

    @Column(nullable = false)
    private Double finalScore;

    // Comma-separated skill names, computed by the matcher - see Step 4.
    @Column(nullable = false)
    private String matchedSkills;

    @Column(nullable = false)
    private String missingSkills;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
