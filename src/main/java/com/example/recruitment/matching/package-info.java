/**
 * The matching engine: the {@code CandidateMatcher} interface and its
 * implementations, plus the {@code MatchResult} value produced by matching.
 * This package is pure business logic. It must never import Kafka, JPA,
 * Spring MVC, or Apache POI types, so that new matching strategies (weighted,
 * semantic, AI-based) can be added without touching any other package.
 */
package com.example.recruitment.matching;
