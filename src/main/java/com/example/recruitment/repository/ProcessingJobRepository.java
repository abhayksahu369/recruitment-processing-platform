package com.example.recruitment.repository;

import com.example.recruitment.domain.ProcessingJob;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface ProcessingJobRepository extends JpaRepository<ProcessingJob, UUID> {

    /**
     * The fix for the counter race identified in Step 7: instead of the
     * application reading processedCandidates, adding one, and writing it
     * back (a classic lost-update race under concurrent consumer
     * threads), this pushes the increment itself into the database as one
     * atomic UPDATE. The database serializes concurrent writers to the
     * same row internally; there is no read-modify-write gap for two
     * threads to race inside, because neither thread ever holds the
     * "current" value in application memory at all.
     * <p>
     * matchedDelta/failedDelta are 0 or 1 - always incrementing
     * processedCandidates by exactly 1 (every outcome counts as
     * "processed"), and at most one of matched/failed alongside it.
     * <p>
     * clearAutomatically = true matters here for a subtler reason than
     * the race itself: a bulk UPDATE like this happens directly against
     * the database and does NOT update any ProcessingJob instance
     * Hibernate may already be holding in this transaction's session
     * cache (the "first-level cache" / identity map) - the very next line
     * in the consumer calls findById() on this same id, and without
     * clearAutomatically, Hibernate would hand back that stale cached
     * instance instead of re-reading the now-updated row, permanently
     * hiding this update from the completion check below. This was a real
     * bug caught by testing at scale: the counters were correct in the
     * database the whole time, but the job never flipped to COMPLETED.
     * <p>
     * flushAutomatically = true is the other half, and its absence caused
     * an even worse real bug: the consumer calls matchResultRepository
     * .save(...) immediately before this method, but Hibernate defers
     * actual INSERTs until flush time by default - it doesn't happen the
     * instant save() returns. A bulk @Modifying query runs as raw SQL
     * that bypasses the session entirely, so without flushAutomatically,
     * that pending MatchResult insert was still sitting unflushed when
     * clearAutomatically's em.clear() ran - which detaches entities
     * without flushing them first. The result: every single MatchResult
     * was silently discarded, never reaching the database at all (proven
     * by an actual test run: 1110 candidates, 1110 saved, 0 match_results
     * rows). flushAutomatically forces Hibernate to write out that
     * pending insert before the bulk update runs, so it's durably
     * committed before anything has a chance to discard it.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE ProcessingJob j SET " +
            "j.processedCandidates = j.processedCandidates + 1, " +
            "j.matchedCandidates = j.matchedCandidates + :matchedDelta, " +
            "j.failedCandidates = j.failedCandidates + :failedDelta " +
            "WHERE j.id = :jobId")
    void recordCandidateOutcome(@Param("jobId") UUID jobId,
                                 @Param("matchedDelta") int matchedDelta,
                                 @Param("failedDelta") int failedDelta);

    /**
     * Also atomic, and also guarded in the WHERE clause (only fires from
     * QUEUED) so it's safe to call unconditionally from every consumer
     * thread without any of them needing to check the current status
     * first - whichever thread's candidate happens to be processed first
     * makes this transition; every other thread's call is a harmless no-op.
     */
    @Modifying
    @Query("UPDATE ProcessingJob j SET j.status = com.example.recruitment.domain.ProcessingStatus.PROCESSING " +
            "WHERE j.id = :jobId AND j.status = com.example.recruitment.domain.ProcessingStatus.QUEUED")
    void markProcessingIfQueued(@Param("jobId") UUID jobId);

    /**
     * Guarded the same way: harmless to call from multiple threads that
     * each independently discover the job looks done, since only the
     * first one's WHERE clause actually matches a row.
     */
    @Modifying
    @Query("UPDATE ProcessingJob j SET j.status = com.example.recruitment.domain.ProcessingStatus.COMPLETED, " +
            "j.completedAt = :completedAt " +
            "WHERE j.id = :jobId AND j.status <> com.example.recruitment.domain.ProcessingStatus.COMPLETED")
    void markCompleted(@Param("jobId") UUID jobId, @Param("completedAt") Instant completedAt);
}
