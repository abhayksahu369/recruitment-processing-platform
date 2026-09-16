package com.example.recruitment.service;

import com.example.recruitment.domain.Candidate;
import com.example.recruitment.domain.ProcessingJob;
import com.example.recruitment.domain.ProcessingStatus;
import com.example.recruitment.dto.CandidateResultResponse;
import com.example.recruitment.dto.CreateProcessingJobResponse;
import com.example.recruitment.dto.CsvSkills;
import com.example.recruitment.dto.ExcelParseResult;
import com.example.recruitment.dto.JobRequirements;
import com.example.recruitment.dto.ParsedCandidate;
import com.example.recruitment.dto.ProcessingJobStatusResponse;
import com.example.recruitment.dto.CandidateProcessingEvent;
import com.example.recruitment.exception.MalformedExcelException;
import com.example.recruitment.exception.ProcessingJobNotFoundException;
import com.example.recruitment.kafka.CandidateEventProducer;
import com.example.recruitment.matching.CandidateMatcher;
import com.example.recruitment.matching.CandidateProfile;
import com.example.recruitment.parser.ExcelParser;
import com.example.recruitment.parser.JobDescriptionParser;
import com.example.recruitment.repository.CandidateRepository;
import com.example.recruitment.repository.MatchResultRepository;
import com.example.recruitment.repository.ProcessingJobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Orchestrates job creation, in two forms:
 * <p>
 * - createProcessingJob: parse, persist, hand every valid candidate to
 *   Kafka, return immediately - the real, production pipeline from Step
 *   6 onward.
 * - createProcessingJobSynchronously: parse, persist, and match every
 *   candidate right here in the request, with no Kafka involved at all -
 *   reintroduced deliberately as a second, explicit code path purely so
 *   the two approaches can be compared side by side (see
 *   POST /api/processing/jobs/sync), not as a replacement for the async
 *   one. This is intentionally the ONLY place CandidateMatcher is called
 *   from the HTTP-facing side of the app; CandidateProcessingConsumer
 *   calls it independently for the async path, and neither of those two
 *   call sites knows the other exists - exactly the decoupling the
 *   CandidateMatcher interface exists to provide.
 */
@Service
public class ProcessingJobService {

    private static final Logger log = LoggerFactory.getLogger(ProcessingJobService.class);

    /**
     * Matches Step 8's threshold in CandidateProcessingConsumer exactly.
     * Duplicated rather than shared: two occurrences of one named
     * constant isn't yet worth inventing a shared home for (the matching
     * package doesn't define "matched" - that's a reporting decision the
     * matcher itself stays deliberately silent on) - see this project's
     * CsvSkills for the pattern of waiting for a third occurrence before
     * extracting something shared.
     */
    private static final double MATCH_THRESHOLD = 0.5;

    private final JobDescriptionParser jobDescriptionParser;
    private final ExcelParser excelParser;
    private final CandidateEventProducer candidateEventProducer;
    private final CandidateMatcher candidateMatcher;
    private final ProcessingJobRepository processingJobRepository;
    private final CandidateRepository candidateRepository;
    private final MatchResultRepository matchResultRepository;

    public ProcessingJobService(
            JobDescriptionParser jobDescriptionParser,
            ExcelParser excelParser,
            CandidateEventProducer candidateEventProducer,
            CandidateMatcher candidateMatcher,
            ProcessingJobRepository processingJobRepository,
            CandidateRepository candidateRepository,
            MatchResultRepository matchResultRepository
    ) {
        this.jobDescriptionParser = jobDescriptionParser;
        this.excelParser = excelParser;
        this.candidateEventProducer = candidateEventProducer;
        this.candidateMatcher = candidateMatcher;
        this.processingJobRepository = processingJobRepository;
        this.candidateRepository = candidateRepository;
        this.matchResultRepository = matchResultRepository;
    }

    /**
     * By the time this method returns, every valid candidate has been
     * persisted and a CandidateProcessingEvent published for it - but
     * none of them have necessarily been matched yet. That's why status
     * is QUEUED, not COMPLETED, now: it's the honest description of what
     * has actually happened. Step 7 adds the consumer that moves a job
     * through PROCESSING and eventually to COMPLETED.
     * <p>
     * @Transactional still wraps the whole method: the job row, every
     * candidate row, and every Kafka publish either all succeed together
     * or (if something throws midway) all roll back - including, thanks
     * to Spring Kafka's transaction support being active only when a
     * KafkaTransactionManager is configured (it isn't here), the sends
     * themselves are fire-and-forget with respect to the DB transaction.
     * We accept that for V1: see this step's write-up for the tradeoff.
     */
    @Transactional
    public CreateProcessingJobResponse createProcessingJob(String jobDescriptionText, MultipartFile excelFile) {
        JobRequirements requirements = jobDescriptionParser.parse(jobDescriptionText);
        ExcelParseResult parseResult = parseExcel(excelFile);

        // processedCandidates starts pre-populated with the excel-validation
        // failures, not zero: those rows are already "done" (failed) before
        // a single Kafka message goes out. That's what makes
        // processedCandidates a uniform "how many of totalCandidates are
        // done, for any reason" counter from the very first moment - Step
        // 8's atomic completion check (below and in the consumer) never
        // needs to special-case "no valid candidates" separately, it's just
        // the case where this starting value already equals totalCandidates.
        ProcessingJob job = processingJobRepository.save(ProcessingJob.builder()
                .jobTitle(requirements.jobTitle())
                .requiredSkills(CsvSkills.format(requirements.requiredSkills()))
                .minExperience(requirements.minExperience())
                .maxExperience(requirements.maxExperience())
                .totalCandidates(parseResult.candidates().size() + parseResult.errors().size())
                .processedCandidates(parseResult.errors().size())
                .failedCandidates(parseResult.errors().size())
                .build());

        for (ParsedCandidate parsed : parseResult.candidates()) {
            Candidate candidate = saveCandidate(job, parsed);
            candidateEventProducer.publish(new CandidateProcessingEvent(job.getId(), candidate.getId()));
        }

        if (job.getProcessedCandidates() >= job.getTotalCandidates()) {
            // Every row failed validation (or the job legitimately has zero
            // candidates) - nothing was published, so nothing will ever
            // arrive at the consumer to move this job forward. Completing
            // it now is the honest state, not a shortcut.
            processingJobRepository.markCompleted(job.getId(), Instant.now());
            job.setStatus(ProcessingStatus.COMPLETED);
        }

        log.info("Created processing job {} ({}): {} candidates, {} rejected at upload, status={}",
                job.getId(), job.getJobTitle(), job.getTotalCandidates(), job.getFailedCandidates(), job.getStatus());

        return new CreateProcessingJobResponse(job.getId(), job.getStatus(), job.getTotalCandidates());
    }

    /**
     * The pre-Kafka approach: every candidate is matched and persisted
     * before this method returns, so the response you get back is
     * already the final one - status is COMPLETED, not QUEUED, because
     * by the time the caller sees this response, it genuinely is. Exists
     * to let you measure the same workload both ways; the production
     * upload path is createProcessingJob, above.
     * <p>
     * @Transactional wraps the whole loop in one transaction, exactly
     * like the original Step 5 implementation did - which is also why
     * this doesn't scale the way the async path does: one very large
     * spreadsheet means one very large transaction, held open for the
     * entire request.
     */
    @Transactional
    public CreateProcessingJobResponse createProcessingJobSynchronously(
            String jobDescriptionText, MultipartFile excelFile) {
        JobRequirements requirements = jobDescriptionParser.parse(jobDescriptionText);
        ExcelParseResult parseResult = parseExcel(excelFile);

        ProcessingJob job = processingJobRepository.save(ProcessingJob.builder()
                .jobTitle(requirements.jobTitle())
                .requiredSkills(CsvSkills.format(requirements.requiredSkills()))
                .minExperience(requirements.minExperience())
                .maxExperience(requirements.maxExperience())
                .totalCandidates(parseResult.candidates().size() + parseResult.errors().size())
                .failedCandidates(parseResult.errors().size())
                .build());

        int matchedCount = 0;
        for (ParsedCandidate parsed : parseResult.candidates()) {
            Candidate candidate = saveCandidate(job, parsed);

            com.example.recruitment.matching.MatchResult outcome = candidateMatcher.match(
                    new CandidateProfile(parsed.skills(), parsed.experience()), requirements);

            matchResultRepository.save(com.example.recruitment.domain.MatchResult.builder()
                    .processingJobId(job.getId())
                    .candidateId(candidate.getId())
                    .skillScore(outcome.skillScore())
                    .experienceScore(outcome.experienceScore())
                    .finalScore(outcome.finalScore())
                    .matchedSkills(CsvSkills.format(outcome.matchedSkills()))
                    .missingSkills(CsvSkills.format(outcome.missingSkills()))
                    .build());

            if (outcome.finalScore() >= MATCH_THRESHOLD) {
                matchedCount++;
            }
        }

        job.setProcessedCandidates(job.getTotalCandidates());
        job.setMatchedCandidates(matchedCount);
        job.setStatus(ProcessingStatus.COMPLETED);
        job.setCompletedAt(Instant.now());
        processingJobRepository.save(job);

        log.info("Synchronously processed job {} ({}): {} candidates, {} matched",
                job.getId(), job.getJobTitle(), job.getTotalCandidates(), matchedCount);

        return new CreateProcessingJobResponse(job.getId(), job.getStatus(), job.getTotalCandidates());
    }

    @Transactional(readOnly = true)
    public ProcessingJobStatusResponse getStatus(UUID jobId) {
        ProcessingJob job = findJobOrThrow(jobId);

        // While still running, "duration" is elapsed-so-far, not a fixed
        // final number - completedAt is null until the job actually
        // finishes, so we measure against now() instead.
        Instant end = job.getCompletedAt() != null ? job.getCompletedAt() : Instant.now();
        long durationMillis = Duration.between(job.getCreatedAt(), end).toMillis();
        double recordsPerSecond = durationMillis > 0
                ? job.getProcessedCandidates() / (durationMillis / 1000.0)
                : 0.0;

        return new ProcessingJobStatusResponse(
                job.getId(),
                job.getStatus(),
                job.getTotalCandidates(),
                job.getProcessedCandidates(),
                job.getMatchedCandidates(),
                job.getFailedCandidates(),
                durationMillis,
                recordsPerSecond
        );
    }

    @Transactional(readOnly = true)
    public List<CandidateResultResponse> getResults(UUID jobId) {
        findJobOrThrow(jobId); // 404s even when the job has zero results yet

        Map<UUID, Candidate> candidatesById = candidateRepository.findByProcessingJob_Id(jobId).stream()
                .collect(Collectors.toMap(Candidate::getId, Function.identity()));

        return matchResultRepository.findByProcessingJobIdOrderByFinalScoreDesc(jobId).stream()
                .map(result -> toResultResponse(result, candidatesById.get(result.getCandidateId())))
                .toList();
    }

    private Candidate saveCandidate(ProcessingJob job, ParsedCandidate parsed) {
        return candidateRepository.save(Candidate.builder()
                .processingJob(job)
                .candidateId(parsed.candidateId())
                .name(parsed.name())
                .email(parsed.email())
                .skills(CsvSkills.format(parsed.skills()))
                .experience(parsed.experience())
                .build());
    }

    private CandidateResultResponse toResultResponse(com.example.recruitment.domain.MatchResult result,
                                                       Candidate candidate) {
        return new CandidateResultResponse(
                candidate.getCandidateId(),
                candidate.getName(),
                roundToOneDecimalPercent(result.getFinalScore()),
                CsvSkills.parse(result.getMatchedSkills()),
                CsvSkills.parse(result.getMissingSkills())
        );
    }

    /** 0.0-1.0 stored ratio -> a 0-100 percentage, e.g. 0.842 -> 84.2. Presentation only. */
    private static double roundToOneDecimalPercent(double ratio) {
        return Math.round(ratio * 1000) / 10.0;
    }

    private ProcessingJob findJobOrThrow(UUID jobId) {
        return processingJobRepository.findById(jobId)
                .orElseThrow(() -> new ProcessingJobNotFoundException(jobId));
    }

    private ExcelParseResult parseExcel(MultipartFile excelFile) {
        try {
            return excelParser.parse(excelFile.getInputStream());
        } catch (IOException e) {
            throw new MalformedExcelException("Unable to read the uploaded file", e);
        }
    }
}
