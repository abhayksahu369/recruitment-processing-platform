package com.example.recruitment.matching;

import com.example.recruitment.dto.JobRequirements;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * V1's deterministic scoring rule: 80% skill overlap, 20% whether
 * experience falls in the job's requested range. No weighting per skill,
 * no partial credit for "almost enough" experience, no synonyms ("JS" vs
 * "JavaScript") - later versions (WeightedCandidateMatcher, a
 * semantic/AI-based matcher) can refine this without any other package
 * needing to change, because they'll all implement {@link CandidateMatcher}.
 */
@Component
public class SimpleCandidateMatcher implements CandidateMatcher {

    private static final double SKILL_WEIGHT = 0.80;
    private static final double EXPERIENCE_WEIGHT = 0.20;

    @Override
    public MatchResult match(CandidateProfile candidate, JobRequirements requirements) {
        // Case-insensitive lookup set: "java" in the spreadsheet should
        // still satisfy a requirement written as "Java".
        Set<String> candidateSkillsLower = candidate.skills().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        List<String> matchedSkills = new ArrayList<>();
        List<String> missingSkills = new ArrayList<>();
        for (String requiredSkill : requirements.requiredSkills()) {
            if (candidateSkillsLower.contains(requiredSkill.toLowerCase())) {
                matchedSkills.add(requiredSkill);
            } else {
                missingSkills.add(requiredSkill);
            }
        }

        // Nothing required -> trivially fully satisfied, rather than a
        // divide-by-zero on an empty requiredSkills list.
        double skillScore = requirements.requiredSkills().isEmpty()
                ? 1.0
                : (double) matchedSkills.size() / requirements.requiredSkills().size();

        boolean withinExperienceRange = candidate.experience() >= requirements.minExperience()
                && candidate.experience() <= requirements.maxExperience();
        double experienceScore = withinExperienceRange ? 1.0 : 0.0;

        double finalScore = skillScore * SKILL_WEIGHT + experienceScore * EXPERIENCE_WEIGHT;

        return new MatchResult(skillScore, experienceScore, finalScore, matchedSkills, missingSkills);
    }
}
