package com.example.recruitment.matching;

import com.example.recruitment.dto.JobRequirements;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class SimpleCandidateMatcherTest {

    private final SimpleCandidateMatcher matcher = new SimpleCandidateMatcher();

    @Test
    void matchesTheWorkedExampleFromTheSpecExactly() {
        // Job requires 5 skills, candidate has 4 of them, within range.
        // skillScore = 4/5 = 0.8, experienceScore = 1.0
        // finalScore = 0.8*0.8 + 1.0*0.2 = 0.84
        JobRequirements requirements = new JobRequirements(
                "Java Backend Developer",
                List.of("Java", "Spring Boot", "SQL", "Kafka", "Docker"),
                0, 2
        );
        CandidateProfile candidate = new CandidateProfile(
                List.of("Java", "Spring Boot", "SQL", "Docker"), 1
        );

        MatchResult result = matcher.match(candidate, requirements);

        assertThat(result.skillScore()).isEqualTo(0.8);
        assertThat(result.experienceScore()).isEqualTo(1.0);
        assertThat(result.finalScore()).isCloseTo(0.84, within(0.0001));
        assertThat(result.matchedSkills()).containsExactly("Java", "Spring Boot", "SQL", "Docker");
        assertThat(result.missingSkills()).containsExactly("Kafka");
    }

    @Test
    void perfectMatch_scoresExactlyOne() {
        JobRequirements requirements = new JobRequirements("Role", List.of("Java", "SQL"), 0, 2);
        CandidateProfile candidate = new CandidateProfile(List.of("Java", "SQL"), 1);

        MatchResult result = matcher.match(candidate, requirements);

        assertThat(result.finalScore()).isEqualTo(1.0);
        assertThat(result.missingSkills()).isEmpty();
    }

    @Test
    void noMatchingSkillsAndOutOfRangeExperience_scoresExactlyZero() {
        JobRequirements requirements = new JobRequirements("Role", List.of("Java", "SQL"), 3, 5);
        CandidateProfile candidate = new CandidateProfile(List.of("Python"), 0);

        MatchResult result = matcher.match(candidate, requirements);

        assertThat(result.skillScore()).isEqualTo(0.0);
        assertThat(result.experienceScore()).isEqualTo(0.0);
        assertThat(result.finalScore()).isEqualTo(0.0);
        assertThat(result.matchedSkills()).isEmpty();
        assertThat(result.missingSkills()).containsExactly("Java", "SQL");
    }

    @Test
    void skillMatchingIsCaseInsensitive() {
        JobRequirements requirements = new JobRequirements("Role", List.of("Java"), 0, 5);
        CandidateProfile candidate = new CandidateProfile(List.of("java"), 1);

        MatchResult result = matcher.match(candidate, requirements);

        assertThat(result.matchedSkills()).containsExactly("Java");
        assertThat(result.skillScore()).isEqualTo(1.0);
    }

    @Test
    void experienceExactlyAtTheRangeBoundaries_countsAsWithinRange() {
        JobRequirements requirements = new JobRequirements("Role", List.of(), 1, 3);

        assertThat(matcher.match(new CandidateProfile(List.of(), 1), requirements).experienceScore())
                .isEqualTo(1.0);
        assertThat(matcher.match(new CandidateProfile(List.of(), 3), requirements).experienceScore())
                .isEqualTo(1.0);
        assertThat(matcher.match(new CandidateProfile(List.of(), 4), requirements).experienceScore())
                .isEqualTo(0.0);
    }

    @Test
    void noRequiredSkills_isTriviallyFullySatisfiedRatherThanDividingByZero() {
        JobRequirements requirements = new JobRequirements("Role", List.of(), 0, 5);
        CandidateProfile candidate = new CandidateProfile(List.of("Anything"), 1);

        MatchResult result = matcher.match(candidate, requirements);

        assertThat(result.skillScore()).isEqualTo(1.0);
        assertThat(result.matchedSkills()).isEmpty();
        assertThat(result.missingSkills()).isEmpty();
    }
}
