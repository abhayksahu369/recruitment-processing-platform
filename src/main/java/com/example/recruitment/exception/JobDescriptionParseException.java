package com.example.recruitment.exception;

/**
 * The job description text doesn't match the deterministic format we
 * expect (title line, "Skills:" section, "Experience:" section). Unlike a
 * bad candidate row, there's no partial success here - either we have
 * requirements to match against or we don't - so this fails the whole
 * job-creation request.
 */
public class JobDescriptionParseException extends RuntimeException {

    public JobDescriptionParseException(String message) {
        super(message);
    }
}
