package com.example.recruitment.repository;

import com.example.recruitment.domain.ProcessingJob;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * No custom queries yet - every operation we need (save a new job, look one
 * up by id for the status endpoint) is already provided by JpaRepository.
 */
public interface ProcessingJobRepository extends JpaRepository<ProcessingJob, UUID> {
}
