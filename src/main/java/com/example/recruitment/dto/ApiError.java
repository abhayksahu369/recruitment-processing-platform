package com.example.recruitment.dto;

/** Uniform error body for every 4xx response this API returns. */
public record ApiError(String message) {
}
