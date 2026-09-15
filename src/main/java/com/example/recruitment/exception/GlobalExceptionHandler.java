package com.example.recruitment.exception;

import com.example.recruitment.dto.ApiError;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

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
}
