package com.example.recruitment.repository;

import com.example.recruitment.domain.Candidate;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface CandidateRepository extends JpaRepository<Candidate, UUID> {

    /**
     * "ProcessingJob_Id" (underscore) tells Spring Data to traverse the
     * processingJob relationship and filter on *its* id property, rather
     * than looking for a flat field literally named processingJobId on
     * Candidate (which doesn't exist - see Candidate.processingJob).
     * Spring Data parses the method name into a query at startup; this
     * becomes roughly:
     *   select c from Candidate c where c.processingJob.id = :processingJobId
     */
    List<Candidate> findByProcessingJob_Id(UUID processingJobId);
}
