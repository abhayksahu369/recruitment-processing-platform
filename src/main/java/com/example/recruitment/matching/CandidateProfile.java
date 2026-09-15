package com.example.recruitment.matching;

import java.util.List;

/**
 * Exactly the data a matcher needs to score a candidate against a job -
 * nothing else. Deliberately not {@code domain.Candidate}: that class
 * carries a database id, an email, a name, and a JPA relationship to
 * ProcessingJob - none of which the matching algorithm has any business
 * looking at. Whoever calls the matcher (the synchronous service in Step
 * 5, or the Kafka consumer in Step 7) builds one of these from whichever
 * candidate representation it actually has on hand - a freshly parsed
 * {@code ParsedCandidate} or a {@code Candidate} loaded from the database.
 */
public record CandidateProfile(List<String> skills, int experience) {
}
