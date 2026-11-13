package dev.jobscheduler.domain;

import java.util.UUID;

/** A task this node has leased. {@code leaseToken} is the fencing token. */
public record ClaimedTask(
        UUID id,
        UUID jobId,
        String name,
        String type,
        String params,
        int attempt,
        int maxAttempts,
        int backoffSeconds,
        long leaseToken) {
}
