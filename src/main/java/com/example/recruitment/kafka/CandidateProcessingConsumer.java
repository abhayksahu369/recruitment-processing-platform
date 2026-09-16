package com.example.recruitment.kafka;

import com.example.recruitment.domain.Candidate;
import com.example.recruitment.domain.ProcessingJob;
import com.example.recruitment.dto.CandidateProcessingEvent;
import com.example.recruitment.dto.CsvSkills;
import com.example.recruitment.dto.JobRequirements;
import com.example.recruitment.matching.CandidateMatcher;
import com.example.recruitment.matching.CandidateProfile;
import com.example.recruitment.repository.CandidateRepository;
import com.example.recruitment.repository.MatchResultRepository;
import com.example.recruitment.repository.ProcessingJobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * The consumer half of the pipeline this whole project exists to teach.
 * Unlike ProcessingJobService, this class talks to CandidateMatcher and
 * the repositories directly - matching the spec's own architecture
 * diagram, where the async path is Consumer -> CandidateMatcher ->
 * MatchResult -> Repository, with no Service in that chain at all.
 * <p>
 * A candidate whose final score clears this bar counts as "matched" - the
 * same threshold and reasoning as Step 5's now-deleted MATCH_THRESHOLD:
 * the spec doesn't pin an exact number, so it's a deliberate, named,
 * easily-revisited constant rather than a number buried in an expression.
 */
@Component
public class CandidateProcessingConsumer {

    private static final Logger log = LoggerFactory.getLogger(CandidateProcessingConsumer.class);
    private static final double MATCH_THRESHOLD = 0.5;

    private final CandidateRepository candidateRepository;
    private final ProcessingJobRepository processingJobRepository;
    private final CandidateMatcher candidateMatcher;
    private final MatchResultRepository matchResultRepository;

    public CandidateProcessingConsumer(
            CandidateRepository candidateRepository,
            ProcessingJobRepository processingJobRepository,
            CandidateMatcher candidateMatcher,
            MatchResultRepository matchResultRepository
    ) {
        this.candidateRepository = candidateRepository;
        this.processingJobRepository = processingJobRepository;
        this.candidateMatcher = candidateMatcher;
        this.matchResultRepository = matchResultRepository;
    }

    /**
     * groupId: consumers sharing this id split the topic's partitions
     * between them - Kafka guarantees each partition is read by at most
     * one member of the group at a time, which is *why* a consumer group
     * is the unit of parallelism, not the individual consumer.
     * <p>
     * concurrency: tells Spring Kafka how many listener threads to run in
     * this one application, each behaving as a separate group member.
     * Pulled from a property (default 3, matching the topic's 3 partitions
     * from Step 6) instead of a hardcoded literal specifically so Step 9's
     * benchmarks can vary it and measure the effect directly, rather than
     * asserting it matters without proof. Concurrency beyond the partition
     * count is wasted - a 4th thread would never be assigned a partition
     * to read from and would sit permanently idle.
     * <p>
     * @Transactional wraps everything below in one DB transaction. It does
     * NOT extend to the Kafka side - see CandidateEventProducer's Javadoc
     * for the producer-side half of that story, and the idempotency check
     * below for why a redelivered message is still handled correctly.
     * <p>
     * This method itself never lets an "ordinary" failure (bad match,
     * missing referenced row) escape as an exception - see matchAndPersist
     * below. That's deliberate: an uncaught exception here would fall back
     * to Kafka's default error handling, which (Step 7 found out the hard
     * way) retries a few times with no backoff and then silently skips the
     * message forever, leaving no record it ever existed and potentially
     * stranding the job below its completion threshold permanently. Catching
     * it and recording it as a counted failure instead means the job can
     * always still reach COMPLETED, and the failure is visible, not lost.
     */
    @KafkaListener(
            topics = KafkaTopics.CANDIDATE_PROCESSING,
            groupId = "candidate-processing-group",
            concurrency = "${recruitment.kafka.consumer-concurrency:3}"
    )
    @Transactional
    public void onCandidateProcessingEvent(CandidateProcessingEvent event) {
        if (matchResultRepository.existsByProcessingJobIdAndCandidateId(
                event.processingJobId(), event.candidateId())) {
            log.info("Skipping already-processed candidate {} for job {} (redelivered message)",
                    event.candidateId(), event.processingJobId());
            return;
        }

        Outcome outcome = matchAndPersist(event);
        if (outcome == Outcome.DUPLICATE) {
            return; // already counted by whichever thread won the DB-level race
        }

        processingJobRepository.recordCandidateOutcome(
                event.processingJobId(),
                outcome == Outcome.MATCHED ? 1 : 0,
                outcome == Outcome.FAILED ? 1 : 0);
        processingJobRepository.markProcessingIfQueued(event.processingJobId());

        ProcessingJob refreshed = processingJobRepository.findById(event.processingJobId()).orElseThrow();
        if (refreshed.getProcessedCandidates() >= refreshed.getTotalCandidates()) {
            processingJobRepository.markCompleted(event.processingJobId(), Instant.now());
        }
    }

    /**
     * Two genuinely different failure sources here, handled differently on
     * purpose:
     * <p>
     * - candidateMatcher.match() throwing, or the referenced job/candidate
     *   not existing (which Step 7's producer fix makes rare, but this
     *   stays defensive) - pure application-level failures. The database
     *   session is completely unaffected by either, so it's safe to catch
     *   them, record a FAILED outcome, and keep going in the same
     *   transaction.
     * - matchResultRepository.save() throwing DataIntegrityViolationException
     *   - the unique constraint (domain.MatchResult) rejecting a duplicate
     *   insert that the exists()-check above didn't catch (two threads
     *   racing on the same redelivered message). This is not a failure to
     *   record - the candidate WAS already scored, by the thread that won.
     * <p>
     * Anything else - a genuine database outage, say - is deliberately NOT
     * caught here. If the database itself is unhealthy, attempting to
     * record a "failed" outcome would fail too, and the exception
     * propagates out of the listener, falling back to Kafka's default
     * retry - the right behavior for a transient infrastructure problem,
     * where retrying might actually succeed once the database recovers.
     * Distinguishing that from a truly permanent failure (and giving up
     * gracefully via a dead letter topic) is exactly the "complex retry/DLT
     * architecture" your spec explicitly keeps out of V1.
     */
    private Outcome matchAndPersist(CandidateProcessingEvent event) {
        double finalScore;
        try {
            ProcessingJob job = processingJobRepository.findById(event.processingJobId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Received an event for unknown processing job " + event.processingJobId()));
            Candidate candidate = candidateRepository.findById(event.candidateId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Received an event for unknown candidate " + event.candidateId()));

            JobRequirements requirements = new JobRequirements(
                    job.getJobTitle(),
                    CsvSkills.parse(job.getRequiredSkills()),
                    job.getMinExperience(),
                    job.getMaxExperience()
            );
            CandidateProfile profile = new CandidateProfile(
                    CsvSkills.parse(candidate.getSkills()), candidate.getExperience());

            com.example.recruitment.matching.MatchResult matched = candidateMatcher.match(profile, requirements);

            matchResultRepository.save(com.example.recruitment.domain.MatchResult.builder()
                    .processingJobId(job.getId())
                    .candidateId(candidate.getId())
                    .skillScore(matched.skillScore())
                    .experienceScore(matched.experienceScore())
                    .finalScore(matched.finalScore())
                    .matchedSkills(CsvSkills.format(matched.matchedSkills()))
                    .missingSkills(CsvSkills.format(matched.missingSkills()))
                    .build());

            finalScore = matched.finalScore();
        } catch (DataIntegrityViolationException duplicate) {
            log.info("Duplicate MatchResult rejected by the database for candidate {} in job {} - already scored",
                    event.candidateId(), event.processingJobId());
            return Outcome.DUPLICATE;
        } catch (RuntimeException failure) {
            log.error("Failed to process candidate {} for job {}: {}",
                    event.candidateId(), event.processingJobId(), failure.getMessage(), failure);
            return Outcome.FAILED;
        }

        return finalScore >= MATCH_THRESHOLD ? Outcome.MATCHED : Outcome.NOT_MATCHED;
    }

    private enum Outcome {
        MATCHED, NOT_MATCHED, FAILED, DUPLICATE
    }
}
