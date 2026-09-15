package com.example.recruitment.kafka;

import com.example.recruitment.domain.Candidate;
import com.example.recruitment.domain.ProcessingJob;
import com.example.recruitment.domain.ProcessingStatus;
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
     * concurrency = "3": tells Spring Kafka to run 3 listener threads in
     * this one application, each behaving as a separate group member.
     * With the topic's 3 partitions (Step 6), this actually engages all of
     * them in parallel - proof that partitions, not just consumers, are
     * the real ceiling on parallelism (1 consumer thread with 3 partitions
     * would idle 2 of them; 3 consumer threads with 1 partition would
     * leave 2 threads permanently idle).
     * <p>
     * @Transactional wraps everything below in one DB transaction - the
     * MatchResult insert and the job's counter update either both commit
     * or neither does. It does NOT extend to the Kafka side: committing
     * this transaction and committing the consumed offset are two
     * separate operations against two separate systems, with no
     * distributed transaction spanning both (that would need Kafka
     * transactions chained to the DB transaction manager - explicitly the
     * kind of complexity your spec keeps out of V1). The practical
     * consequence: a crash between this method returning and the offset
     * commit means Kafka redelivers the message - which is exactly why
     * the idempotency check below exists.
     */
    @KafkaListener(topics = KafkaTopics.CANDIDATE_PROCESSING, groupId = "candidate-processing-group", concurrency = "3")
    @Transactional
    public void onCandidateProcessingEvent(CandidateProcessingEvent event) {
        if (matchResultRepository.existsByProcessingJobIdAndCandidateId(
                event.processingJobId(), event.candidateId())) {
            log.info("Skipping already-processed candidate {} for job {} (redelivered message)",
                    event.candidateId(), event.processingJobId());
            return;
        }

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

        com.example.recruitment.matching.MatchResult outcome = candidateMatcher.match(profile, requirements);

        try {
            matchResultRepository.save(com.example.recruitment.domain.MatchResult.builder()
                    .processingJobId(job.getId())
                    .candidateId(candidate.getId())
                    .skillScore(outcome.skillScore())
                    .experienceScore(outcome.experienceScore())
                    .finalScore(outcome.finalScore())
                    .matchedSkills(CsvSkills.format(outcome.matchedSkills()))
                    .missingSkills(CsvSkills.format(outcome.missingSkills()))
                    .build());
        } catch (DataIntegrityViolationException duplicate) {
            // The rare race the exists()-check above didn't catch: another
            // thread inserted this same candidate's result microseconds
            // earlier. The unique constraint (see domain.MatchResult) just
            // did its job - treat it exactly like the check above would
            // have, and don't touch the counters a second time.
            log.info("Duplicate MatchResult rejected by the database for candidate {} in job {} - already scored",
                    event.candidateId(), event.processingJobId());
            return;
        }

        updateJobProgress(job, outcome.finalScore() >= MATCH_THRESHOLD);
    }

    /**
     * Deliberately naive for this step: read the job's counters, add one,
     * write them back. Under real concurrency (three threads, per the
     * @KafkaListener concurrency above, all potentially updating the same
     * job's row at once) this is a textbook lost-update race - two threads
     * can both read processedCandidates=41, both compute 42, and one
     * increment vanishes. Step 7's write-up reports what actually happens
     * when this runs at scale; Step 8 is where this gets fixed properly.
     */
    private void updateJobProgress(ProcessingJob job, boolean matched) {
        job.setProcessedCandidates(job.getProcessedCandidates() + 1);
        if (matched) {
            job.setMatchedCandidates(job.getMatchedCandidates() + 1);
        }
        if (job.getStatus() == ProcessingStatus.QUEUED) {
            job.setStatus(ProcessingStatus.PROCESSING);
        }
        if (job.getProcessedCandidates() + job.getFailedCandidates() >= job.getTotalCandidates()) {
            job.setStatus(ProcessingStatus.COMPLETED);
            job.setCompletedAt(Instant.now());
        }
        processingJobRepository.save(job);
    }
}
