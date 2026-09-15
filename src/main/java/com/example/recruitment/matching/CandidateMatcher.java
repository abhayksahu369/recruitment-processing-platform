package com.example.recruitment.matching;

import com.example.recruitment.dto.JobRequirements;

/**
 * The one seam in this codebase where "how good is this match" is decided.
 * Every other package - controllers, the Kafka consumer, the repository -
 * depends on this interface, never on a specific implementation
 * (Dependency Inversion). That's what makes it possible to add
 * WeightedCandidateMatcher, or a semantic/AI-based matcher, in a future
 * version by writing a new class and changing which bean gets injected -
 * zero changes to Kafka, controllers, or persistence code (Open/Closed).
 */
public interface CandidateMatcher {

    MatchResult match(CandidateProfile candidate, JobRequirements requirements);
}
