package com.example.recruitment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.UUID;

/**
 * One row from an uploaded candidate spreadsheet. Always belongs to exactly
 * one {@link ProcessingJob} - the same person could be uploaded as part of
 * several different jobs over time, and the spreadsheet's own
 * {@code candidate_id} (e.g. "C001") is only unique within a single upload,
 * not across jobs. The database enforces that pairing directly (see the
 * unique constraint below) rather than trusting application code alone.
 */
@Entity
@Table(
        name = "candidates",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_candidate_job_candidate_id",
                columnNames = {"processing_job_id", "candidate_id"}
        )
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Candidate {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // LAZY: loading a Candidate should not silently pull its ProcessingJob
    // along too. Code that needs job details fetches it explicitly.
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "processing_job_id", nullable = false)
    private ProcessingJob processingJob;

    // The spreadsheet's own id column, e.g. "C001" - kept distinct from the
    // generated `id` above, which is this row's real, globally unique key.
    @Column(name = "candidate_id", nullable = false)
    private String candidateId;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String email;

    // Comma-separated, same shape as the "skills" column in the spreadsheet.
    @Column(nullable = false)
    private String skills;

    @Column(nullable = false)
    private Integer experience;
}
