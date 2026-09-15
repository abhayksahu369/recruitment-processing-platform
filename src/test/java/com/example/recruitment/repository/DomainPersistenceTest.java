package com.example.recruitment.repository;

import com.example.recruitment.domain.Candidate;
import com.example.recruitment.domain.MatchResult;
import com.example.recruitment.domain.ProcessingJob;
import com.example.recruitment.domain.ProcessingStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @DataJpaTest boots only the JPA slice of the application (entities,
 * repositories, the datasource, Hibernate) instead of the whole Spring
 * context - much faster than @SpringBootTest, and appropriate here because
 * we're only testing persistence, not HTTP or Kafka. Each test method runs
 * inside its own transaction that's rolled back afterwards, so tests never
 * leak data into one another even though they share one H2 instance.
 */
@DataJpaTest
class DomainPersistenceTest {

    @Autowired
    private ProcessingJobRepository processingJobRepository;

    @Autowired
    private CandidateRepository candidateRepository;

    @Autowired
    private MatchResultRepository matchResultRepository;

    @Test
    void savingAJob_assignsIdAndDefaultsStatusAndCreatedAt() {
        ProcessingJob job = ProcessingJob.builder()
                .jobTitle("Java Backend Developer")
                .requiredSkills("Java, Spring Boot, SQL, Kafka, Docker")
                .minExperience(0)
                .maxExperience(2)
                .build();

        ProcessingJob saved = processingJobRepository.saveAndFlush(job);

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getStatus()).isEqualTo(ProcessingStatus.QUEUED);
        assertThat(saved.getCreatedAt()).isNotNull();
    }

    @Test
    void candidatesAreFoundByTheirProcessingJob() {
        ProcessingJob job = processingJobRepository.saveAndFlush(aJob());

        candidateRepository.saveAndFlush(aCandidate(job, "C001"));
        candidateRepository.saveAndFlush(aCandidate(job, "C002"));

        List<Candidate> found = candidateRepository.findByProcessingJob_Id(job.getId());

        assertThat(found).hasSize(2)
                .extracting(Candidate::getCandidateId)
                .containsExactlyInAnyOrder("C001", "C002");
    }

    @Test
    void sameCandidateIdTwiceInTheSameJob_violatesTheUniqueConstraint() {
        ProcessingJob job = processingJobRepository.saveAndFlush(aJob());
        candidateRepository.saveAndFlush(aCandidate(job, "C001"));

        assertThrows(DataIntegrityViolationException.class,
                () -> candidateRepository.saveAndFlush(aCandidate(job, "C001")));
    }

    @Test
    void matchResultsComeBackRankedByFinalScoreDescending() {
        ProcessingJob job = processingJobRepository.saveAndFlush(aJob());

        matchResultRepository.saveAndFlush(aMatchResult(job.getId(), 0.65));
        matchResultRepository.saveAndFlush(aMatchResult(job.getId(), 0.92));
        matchResultRepository.saveAndFlush(aMatchResult(job.getId(), 0.40));

        List<MatchResult> ranked =
                matchResultRepository.findByProcessingJobIdOrderByFinalScoreDesc(job.getId());

        assertThat(ranked).extracting(MatchResult::getFinalScore)
                .containsExactly(0.92, 0.65, 0.40);
    }

    private static ProcessingJob aJob() {
        return ProcessingJob.builder()
                .jobTitle("Java Backend Developer")
                .requiredSkills("Java, Spring Boot, SQL, Kafka, Docker")
                .minExperience(0)
                .maxExperience(2)
                .build();
    }

    private static Candidate aCandidate(ProcessingJob job, String candidateId) {
        return Candidate.builder()
                .processingJob(job)
                .candidateId(candidateId)
                .name("Rahul")
                .email("rahul@email.com")
                .skills("Java, Spring Boot, SQL")
                .experience(1)
                .build();
    }

    private static MatchResult aMatchResult(UUID processingJobId, double finalScore) {
        return MatchResult.builder()
                .processingJobId(processingJobId)
                .candidateId(UUID.randomUUID())
                .skillScore(finalScore)
                .experienceScore(1.0)
                .finalScore(finalScore)
                .matchedSkills("Java, Spring Boot")
                .missingSkills("Kafka")
                .build();
    }
}
