package dev.jobscheduler.domain;

public enum JobStatus {
    PENDING, RUNNING, SUCCEEDED, FAILED, CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
