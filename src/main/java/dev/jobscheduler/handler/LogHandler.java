package dev.jobscheduler.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** type "log": logs params.message. */
@Component
public class LogHandler implements TaskHandler {
    private static final Logger log = LoggerFactory.getLogger(LogHandler.class);

    @Override
    public String type() {
        return "log";
    }

    @Override
    public String execute(TaskContext ctx) {
        String message = ctx.params().string("message", "(no message)");
        log.info("[{}/{}] {}", ctx.jobId(), ctx.name(), message);
        return message;
    }
}
