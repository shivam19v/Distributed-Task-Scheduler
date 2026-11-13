package dev.jobscheduler.handler;

import java.time.Duration;

import org.springframework.stereotype.Component;

/** type "sleep": sleeps params.seconds (default 1). Handy for demoing worker death / cancellation. */
@Component
public class SleepHandler implements TaskHandler {

    @Override
    public String type() {
        return "sleep";
    }

    @Override
    public String execute(TaskContext ctx) throws InterruptedException {
        double seconds = ctx.params().decimal("seconds", 1);
        Thread.sleep(Duration.ofMillis((long) (seconds * 1000)));
        return "slept " + seconds + "s";
    }
}
