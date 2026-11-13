package dev.jobscheduler.handler;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Locale;

import org.springframework.stereotype.Component;

import dev.jobscheduler.config.SchedulerProperties;

/**
 * type "http": params.url (required), params.method (GET), params.body, params.timeoutSeconds (10).
 * 5xx / 429 / I/O errors are retried; other 4xx fail immediately.
 * <p>
 * SECURITY: this makes the scheduler issue arbitrary outbound requests. In any real deployment set
 * {@code scheduler.http-allowed-hosts} to an allow-list (and block link-local / metadata addresses).
 */
@Component
public class HttpHandler implements TaskHandler {

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final List<String> allowedHosts;

    public HttpHandler(SchedulerProperties props) {
        this.allowedHosts = props.httpAllowedHosts();
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public String execute(TaskContext ctx) throws Exception {
        String url = ctx.params().string("url", null);
        if (url == null) {
            throw new NonRetryableException("params.url is required");
        }
        URI uri = URI.create(url);
        if (!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) {
            throw new NonRetryableException("Only http/https URLs are allowed");
        }
        if (!allowedHosts.isEmpty() && !allowedHosts.contains(uri.getHost())) {
            throw new NonRetryableException("Host not in scheduler.http-allowed-hosts: " + uri.getHost());
        }

        String method = ctx.params().string("method", "GET").toUpperCase(Locale.ROOT);
        String body = ctx.params().string("body", null);
        HttpRequest.Builder req = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(ctx.params().integer("timeoutSeconds", 10)));
        if (body != null) {
            req.header("Content-Type", "application/json");
            req.method(method, HttpRequest.BodyPublishers.ofString(body));
        } else {
            req.method(method, HttpRequest.BodyPublishers.noBody());
        }

        HttpResponse<String> resp = client.send(req.build(), HttpResponse.BodyHandlers.ofString());
        int status = resp.statusCode();
        if (status >= 500 || status == 429) {
            throw new IOException("HTTP " + status);
        }
        if (status >= 400) {
            throw new NonRetryableException("HTTP " + status);
        }
        String b = resp.body() == null ? "" : resp.body();
        return "HTTP " + status + (b.isEmpty() ? "" : " " + (b.length() > 500 ? b.substring(0, 500) : b));
    }
}
