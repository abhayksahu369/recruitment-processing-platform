package com.example.recruitment.matching;

import java.util.List;

/**
 * The pure output of {@link CandidateMatcher#match}: how well one candidate
 * matched one job's requirements, and nothing about who that candidate or
 * job actually is. skillScore and experienceScore are 0.0-1.0 ratios;
 * finalScore is their weighted sum (see SimpleCandidateMatcher).
 * <p>
 * This is a different class from {@code domain.MatchResult}, on purpose.
 * That one is a JPA entity with an id, a processingJobId, a candidateId,
 * and a createdAt timestamp - persistence concerns the matcher has no
 * reason to know about. The service layer (Step 5) is what takes one of
 * these and turns it into a row in the database, attaching the identity
 * information the matcher was never given.
 */
public record MatchResult(
        double skillScore,
        double experienceScore,
        double finalScore,
        List<String> matchedSkills,
        List<String> missingSkills
) {
}
