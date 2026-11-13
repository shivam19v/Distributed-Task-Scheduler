package dev.jobscheduler.domain;

public class SchedulerException extends RuntimeException {
    public SchedulerException(String message) {
        super(message);
    }

    public static class NotFound extends SchedulerException {
        public NotFound(String message) { super(message); }
    }

    public static class Conflict extends SchedulerException {
        public Conflict(String message) { super(message); }
    }

    public static class InvalidRequest extends SchedulerException {
        public InvalidRequest(String message) { super(message); }
    }
}
