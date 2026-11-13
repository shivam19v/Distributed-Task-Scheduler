package dev.jobscheduler.domain;

public enum TaskStatus {
    /** Waiting for upstream tasks. */
    PENDING,
    /** Dependencies satisfied; eligible to be claimed by a worker. */
    READY,
    /** Leased by a worker. */
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    /** Never ran because an upstream task failed or was cancelled. */
    SKIPPED
}
