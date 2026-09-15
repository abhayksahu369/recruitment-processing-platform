package com.example.recruitment.repository;

import com.example.recruitment.domain.MatchResult;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface MatchResultRepository extends JpaRepository<MatchResult, UUID> {

    /**
     * Backs GET /api/processing/jobs/{jobId}/results (Step 5/9). No
     * underscore needed here - unlike Candidate, processingJobId is a flat
     * UUID column on MatchResult (see MatchResult's Javadoc for why), so
     * this reads directly as:
     *   select m from MatchResult m
     *   where m.processingJobId = :processingJobId
     *   order by m.finalScore desc
     */
    List<MatchResult> findByProcessingJobIdOrderByFinalScoreDesc(UUID processingJobId);
}
