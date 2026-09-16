package com.example.recruitment.benchmark;

import com.example.recruitment.domain.Candidate;
import com.example.recruitment.domain.ProcessingJob;
import com.example.recruitment.dto.CsvSkills;
import com.example.recruitment.dto.ExcelParseResult;
import com.example.recruitment.dto.JobRequirements;
import com.example.recruitment.dto.ParsedCandidate;
import com.example.recruitment.matching.CandidateMatcher;
import com.example.recruitment.matching.CandidateProfile;
import com.example.recruitment.repository.CandidateRepository;
import com.example.recruitment.repository.MatchResultRepository;
import com.example.recruitment.repository.ProcessingJobRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Test-only reconstruction of Step 5's original synchronous pipeline
 * (before Kafka existed at all): parse, then loop - save candidate,
 * match, save result - all inside one transaction, exactly like
 * ProcessingJobService.createProcessingJob used to work. This exists
 * purely so SynchronousVsAsyncBenchmarkTest has a genuine "what we had
 * before" baseline to measure against - it is deliberately NOT part of
 * the production src/main code, since Step 6 onward the real application
 * only processes candidates through Kafka.
 * <p>
 * @Component (not just a plain class instantiated with `new`) matters
 * here: @Transactional only works through Spring's proxy, and calling a
 * method on `this` from inside the test class would bypass that proxy
 * entirely (Spring AOP's well-known self-invocation limitation). Making
 * this a separate, real Spring bean and calling it from the test through
 * dependency injection is what makes the one-big-transaction behavior
 * genuine, not simulated.
 */
@Component
class SynchronousCandidateProcessor {

    private static final double MATCH_THRESHOLD = 0.5;

    private final CandidateRepository candidateRepository;
    private final ProcessingJobRepository processingJobRepository;
    private final CandidateMatcher candidateMatcher;
    private final MatchResultRepository matchResultRepository;

    SynchronousCandidateProcessor(
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

    /** Returns the job id, so the caller can verify results afterward. */
    @Transactional
    UUID processSynchronously(JobRequirements requirements, ExcelParseResult parseResult) {
        ProcessingJob job = processingJobRepository.save(ProcessingJob.builder()
                .jobTitle(requirements.jobTitle())
                .requiredSkills(CsvSkills.format(requirements.requiredSkills()))
                .minExperience(requirements.minExperience())
                .maxExperience(requirements.maxExperience())
                .totalCandidates(parseResult.candidates().size())
                .build());

        for (ParsedCandidate parsed : parseResult.candidates()) {
            Candidate candidate = candidateRepository.save(Candidate.builder()
                    .processingJob(job)
                    .candidateId(parsed.candidateId())
                    .name(parsed.name())
                    .email(parsed.email())
                    .skills(CsvSkills.format(parsed.skills()))
                    .experience(parsed.experience())
                    .build());

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
        }

        return job.getId();
    }
}
