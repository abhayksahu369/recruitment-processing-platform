package com.example.recruitment.controller;

import com.example.recruitment.dto.CandidateResultResponse;
import com.example.recruitment.dto.CreateProcessingJobResponse;
import com.example.recruitment.dto.ProcessingJobStatusResponse;
import com.example.recruitment.service.ProcessingJobService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.UUID;

/**
 * HTTP boundary only: every method here does argument binding and delegates
 * straight to ProcessingJobService. No parsing, matching, or persistence
 * logic belongs in this class - see the controller package-info for why.
 * The single constructor is Spring's injection point; no @Autowired needed
 * on it (Spring has auto-detected a class's sole constructor since 4.3).
 */
@RestController
@RequestMapping("/api/processing/jobs")
public class ProcessingJobController {

    private final ProcessingJobService processingJobService;

    public ProcessingJobController(ProcessingJobService processingJobService) {
        this.processingJobService = processingJobService;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<CreateProcessingJobResponse> createJob(
            @RequestParam("jobDescription") String jobDescription,
            @RequestParam("candidates") MultipartFile candidatesFile
    ) {
        CreateProcessingJobResponse response =
                processingJobService.createProcessingJob(jobDescription, candidatesFile);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Same request shape as the async endpoint above - same field names,
     * same multipart layout - but this one blocks until every candidate
     * is actually matched, and the response you get back already has
     * status COMPLETED. Exists to let you compare the two approaches on
     * the same data through Postman/curl directly, not as a replacement
     * for the real (async) upload path.
     */
    @PostMapping(value = "/sync", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<CreateProcessingJobResponse> createJobSynchronously(
            @RequestParam("jobDescription") String jobDescription,
            @RequestParam("candidates") MultipartFile candidatesFile
    ) {
        CreateProcessingJobResponse response =
                processingJobService.createProcessingJobSynchronously(jobDescription, candidatesFile);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{jobId}")
    public ProcessingJobStatusResponse getStatus(@PathVariable UUID jobId) {
        return processingJobService.getStatus(jobId);
    }

    @GetMapping("/{jobId}/results")
    public List<CandidateResultResponse> getResults(@PathVariable UUID jobId) {
        return processingJobService.getResults(jobId);
    }
}
