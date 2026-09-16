package com.example.recruitment.exception;

import com.example.recruitment.dto.ApiError;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;

/**
 * The one place HTTP status codes get decided for these exceptions.
 * Without this, an unhandled JobDescriptionParseException would bubble up
 * as a generic 500 Internal Server Error - technically not wrong, but
 * misleading: a malformed job description is the caller's mistake (400),
 * not a server failure. @RestControllerAdvice applies these
 * @ExceptionHandler methods to every @RestController in the application,
 * so individual controllers never need their own try/catch for this.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(JobDescriptionParseException.class)
    public ResponseEntity<ApiError> handleJobDescriptionParseException(JobDescriptionParseException ex) {
        return ResponseEntity.badRequest().body(new ApiError(ex.getMessage()));
    }

    @ExceptionHandler(MalformedExcelException.class)
    public ResponseEntity<ApiError> handleMalformedExcelException(MalformedExcelException ex) {
        return ResponseEntity.badRequest().body(new ApiError(ex.getMessage()));
    }

    @ExceptionHandler(ProcessingJobNotFoundException.class)
    public ResponseEntity<ApiError> handleProcessingJobNotFoundException(ProcessingJobNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ApiError(ex.getMessage()));
    }

    /**
     * A missing "candidates" or "jobDescription" multipart part - e.g. a
     * client that forgot to attach the file. Spring already answers this
     * with 400 by default, but with its own generic error body shape
     * instead of our ApiError - this handler exists purely so every 4xx
     * this API returns looks the same to a caller, not to change the
     * status code Spring already picked correctly.
     */
    @ExceptionHandler(MissingServletRequestPartException.class)
    public ResponseEntity<ApiError> handleMissingServletRequestPartException(
            MissingServletRequestPartException ex) {
        return ResponseEntity.badRequest()
                .body(new ApiError("Missing required part: " + ex.getRequestPartName()));
    }

    /** Same reasoning as above, for a missing plain (non-file) request parameter. */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ApiError> handleMissingServletRequestParameterException(
            MissingServletRequestParameterException ex) {
        return ResponseEntity.badRequest()
                .body(new ApiError("Missing required parameter: " + ex.getParameterName()));
    }
}
