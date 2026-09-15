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
import com.example.recruitment.exception.MalformedExcelException;
import com.example.recruitment.exception.ProcessingJobNotFoundException;
import com.example.recruitment.matching.CandidateMatcher;
import com.example.recruitment.matching.CandidateProfile;
import com.example.recruitment.parser.ExcelParser;
import com.example.recruitment.parser.JobDescriptionParser;
import com.example.recruitment.repository.CandidateRepository;
import com.example.recruitment.repository.MatchResultRepository;
import com.example.recruitment.repository.ProcessingJobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Orchestrates the "create and run a processing job" use case: parse the
 * job description and spreadsheet, persist the job and its candidates,
 * score every valid candidate, and persist those results - all as one
 * unit of work (see the @Transactional note on createProcessingJob).
 * <p>
 * This class is the only thing in the codebase that talks to the parser
 * package, the matching package, and the repository package all at once.
 * That's deliberate: it's the orchestrator, not any of the things it
 * orchestrates - none of those packages know about each other.
 */
@Service
public class ProcessingJobService {

    /**
     * A candidate whose final score clears this bar counts as "matched"
     * for the totalCandidates/matched/failed counters (section 11). The
     * spec doesn't pin an exact number, so this is a deliberate, named,
     * easily-revisited choice for V1 rather than a number buried in an
     * expression somewhere.
     */
    private static final double MATCH_THRESHOLD = 0.5;

    private final JobDescriptionParser jobDescriptionParser;
    private final ExcelParser excelParser;
    private final CandidateMatcher candidateMatcher;
    private final ProcessingJobRepository processingJobRepository;
    private final CandidateRepository candidateRepository;
    private final MatchResultRepository matchResultRepository;

    public ProcessingJobService(
            JobDescriptionParser jobDescriptionParser,
            ExcelParser excelParser,
            CandidateMatcher candidateMatcher,
            ProcessingJobRepository processingJobRepository,
            CandidateRepository candidateRepository,
            MatchResultRepository matchResultRepository
    ) {
        this.jobDescriptionParser = jobDescriptionParser;
        this.excelParser = excelParser;
        this.candidateMatcher = candidateMatcher;
        this.processingJobRepository = processingJobRepository;
        this.candidateRepository = candidateRepository;
        this.matchResultRepository = matchResultRepository;
    }

    /**
     * Runs the entire pipeline synchronously: by the time this method
     * returns, every valid candidate has already been matched and saved -
     * there is no background work left. @Transactional makes the whole
     * thing one database transaction: if anything throws partway through
     * (a DB error on candidate #1,500 of 2,000, say), everything already
     * written in this call rolls back rather than leaving a job half
     * populated. Step 6/7 will change this method's job to "enqueue work",
     * not "do the work" - at which point this same guarantee is provided
     * per-candidate by Kafka instead of per-request by this transaction.
     */
    @Transactional
    public CreateProcessingJobResponse createProcessingJob(String jobDescriptionText, MultipartFile excelFile) {
        JobRequirements requirements = jobDescriptionParser.parse(jobDescriptionText);
        ExcelParseResult parseResult = parseExcel(excelFile);

        ProcessingJob job = processingJobRepository.save(ProcessingJob.builder()
                .jobTitle(requirements.jobTitle())
                .requiredSkills(CsvSkills.format(requirements.requiredSkills()))
                .minExperience(requirements.minExperience())
                .maxExperience(requirements.maxExperience())
                .totalCandidates(parseResult.candidates().size() + parseResult.errors().size())
                .build());

        int matchedCount = 0;
        for (ParsedCandidate parsed : parseResult.candidates()) {
            Candidate candidate = saveCandidate(job, parsed);
            boolean matched = matchAndSave(job, candidate, parsed, requirements);
            if (matched) {
                matchedCount++;
            }
        }

        job.setProcessedCandidates(job.getTotalCandidates());
        job.setFailedCandidates(parseResult.errors().size());
        job.setMatchedCandidates(matchedCount);
        job.setStatus(ProcessingStatus.COMPLETED);
        job.setCompletedAt(Instant.now());
        processingJobRepository.save(job);

        return new CreateProcessingJobResponse(job.getId(), job.getStatus(), job.getTotalCandidates());
    }

    @Transactional(readOnly = true)
    public ProcessingJobStatusResponse getStatus(UUID jobId) {
        ProcessingJob job = findJobOrThrow(jobId);
        return new ProcessingJobStatusResponse(
                job.getId(),
                job.getStatus(),
                job.getTotalCandidates(),
                job.getProcessedCandidates(),
                job.getMatchedCandidates(),
                job.getFailedCandidates()
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

    /** Returns true when this candidate's score cleared MATCH_THRESHOLD. */
    private boolean matchAndSave(ProcessingJob job, Candidate candidate, ParsedCandidate parsed,
                                  JobRequirements requirements) {
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

        return outcome.finalScore() >= MATCH_THRESHOLD;
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
