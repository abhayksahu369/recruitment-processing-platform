package com.example.recruitment.exception;

/**
 * The uploaded file itself is broken - no header row, not a real workbook,
 * corrupted, etc. Distinct from a single bad data row (see
 * {@code CandidateRowError}): a malformed file means we can't even start
 * processing, so unlike bad rows, this fails the whole upload immediately.
 * Unchecked, like the other exceptions here - Spring's exception handling
 * (a future @RestControllerAdvice, Step 5+) maps it to an HTTP response;
 * forcing every caller to declare `throws` would only add ceremony.
 */
public class MalformedExcelException extends RuntimeException {

    public MalformedExcelException(String message) {
        super(message);
    }

    public MalformedExcelException(String message, Throwable cause) {
        super(message, cause);
    }
}
