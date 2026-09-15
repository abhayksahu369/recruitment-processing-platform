package com.example.recruitment.dto;

import java.util.List;

/**
 * Outcome of parsing an entire candidate spreadsheet: the rows that were
 * valid, and the rows that weren't - together, not one-or-the-other. This
 * is what makes "invalid candidates should not crash the entire processing
 * job" possible: the caller decides what to do with candidates() and can
 * still record errors() as failures without the two lists ever colliding.
 */
public record ExcelParseResult(List<ParsedCandidate> candidates, List<CandidateRowError> errors) {
}
