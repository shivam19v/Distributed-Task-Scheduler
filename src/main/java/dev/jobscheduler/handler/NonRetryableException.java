package dev.jobscheduler.handler;

/** Throw from a handler to fail the task immediately without consuming remaining retries. */
public class NonRetryableException extends Exception {
    public NonRetryableException(String message) {
        super(message);
    }
}
