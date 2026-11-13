package dev.jobscheduler.domain;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record TaskView(
        UUID id,
        String name,
        String type,
        TaskStatus status,
        int attempt,
        int maxAttempts,
        String leaseOwner,
        String result,
        String error,
        List<String> dependsOn,
        Instant startedAt,
        Instant finishedAt) {
}
