package com.example.recruitment.exception;

import java.util.UUID;

public class ProcessingJobNotFoundException extends RuntimeException {

    public ProcessingJobNotFoundException(UUID jobId) {
        super("No processing job found with id " + jobId);
    }
}
