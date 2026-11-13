package dev.jobscheduler.handler;

import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Component;

/**
 * type "flaky": for demoing retries.
 * params.failUntilAttempt: fail deterministically while attempt &lt; N (e.g. 3 => fails twice, then succeeds)
 * params.failureRate: otherwise fail randomly with this probability (0..1)
 */
@Component
public class FlakyHandler implements TaskHandler {

    @Override
    public String type() {
        return "flaky";
    }

    @Override
    public String execute(TaskContext ctx) {
        int failUntil = ctx.params().integer("failUntilAttempt", 0);
        double rate = ctx.params().decimal("failureRate", 0);
        if (ctx.attempt() < failUntil || ThreadLocalRandom.current().nextDouble() < rate) {
            throw new IllegalStateException("Simulated failure on attempt " + ctx.attempt());
        }
        return "succeeded on attempt " + ctx.attempt();
    }
}
