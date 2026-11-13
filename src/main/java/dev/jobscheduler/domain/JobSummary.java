package dev.jobscheduler.domain;

import java.time.Instant;
import java.util.UUID;

public record JobSummary(UUID id, String name, JobStatus status, Instant createdAt, Instant finishedAt) {
}
