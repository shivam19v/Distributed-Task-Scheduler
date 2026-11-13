package dev.jobscheduler.config;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        @DefaultValue("") String nodeId,
        @DefaultValue("8") int workerConcurrency,
        @DefaultValue("30s") Duration leaseDuration,
        @DefaultValue("10s") Duration heartbeatInterval,
        @DefaultValue("500ms") Duration pollInterval,
        @DefaultValue("1s") Duration coordinatorInterval,
        @DefaultValue("5s") Duration leaderTtl,
        @DefaultValue("25s") Duration drainTimeout,
        @DefaultValue List<String> httpAllowedHosts) {

    public SchedulerProperties {
        if (nodeId == null || nodeId.isBlank()) {
            nodeId = hostname() + "-" + UUID.randomUUID().toString().substring(0, 6);
        }
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "node";
        }
    }
}
