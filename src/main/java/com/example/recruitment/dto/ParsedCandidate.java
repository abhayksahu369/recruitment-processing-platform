package com.example.recruitment.dto;

import java.util.List;

/**
 * One successfully validated row from an uploaded candidate spreadsheet.
 * Deliberately not the {@code Candidate} entity: this record has no
 * ProcessingJob to belong to yet, and no id - it's raw, structured output
 * from parsing, ready for the service layer (Step 5) to turn into a
 * Candidate once it knows which job it belongs to.
 */
public record ParsedCandidate(
        String candidateId,
        String name,
        String email,
        List<String> skills,
        int experience
) {
}
