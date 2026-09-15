package com.example.recruitment.dto;

/**
 * Why one row of the uploaded spreadsheet was rejected. rowNumber is
 * 1-based and counts the header row, matching what a recruiter would see
 * if they opened the file in Excel - row 1 is the header, so the first
 * data row is row 2.
 */
public record CandidateRowError(int rowNumber, String reason) {
}
