package dev.jobscheduler.domain;

import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record TaskSpec(
        @NotBlank @Size(max = 100) String name,
        @NotBlank @Size(max = 50) String type,
        Map<String, Object> params,
        List<String> dependsOn,
        @Min(1) @Max(20) Integer maxAttempts,
        @Min(0) @Max(3600) Integer backoffSeconds,
        @Min(-100) @Max(100) Integer priority) {
}
