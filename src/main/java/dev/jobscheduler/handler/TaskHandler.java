package dev.jobscheduler.handler;

/**
 * Extension point: implement and register as a Spring bean to add a new task {@code type}.
 * Handlers must be interruptible (respond to {@link InterruptedException}) so that cancelled
 * or lease-lost tasks stop promptly. They should also be idempotent: delivery is at-least-once.
 */
public interface TaskHandler {

    /** The value of {@code type} in a task spec that routes to this handler. */
    String type();

    /** @return a short result string (stored on the task); throw to fail the attempt. */
    String execute(TaskContext ctx) throws Exception;
}
