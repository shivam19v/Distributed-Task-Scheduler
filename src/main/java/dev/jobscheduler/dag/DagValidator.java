package dev.jobscheduler.dag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import dev.jobscheduler.domain.SchedulerException;

/** Validates a task graph at submission time (Kahn's algorithm). */
public final class DagValidator {

    public record Node(String name, List<String> dependsOn) {
        public Node {
            dependsOn = dependsOn == null ? List.of() : dependsOn;
        }
    }

    private DagValidator() {
    }

    /**
     * @return task names in a valid topological order
     * @throws SchedulerException.InvalidRequest on duplicate names, unknown/self dependencies or cycles
     */
    public static List<String> validate(List<Node> nodes) {
        Map<String, Set<String>> deps = new LinkedHashMap<>();
        for (Node n : nodes) {
            if (deps.putIfAbsent(n.name(), new HashSet<>(n.dependsOn())) != null) {
                throw new SchedulerException.InvalidRequest("Duplicate task name: " + n.name());
            }
        }

        Map<String, List<String>> dependents = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();
        for (Map.Entry<String, Set<String>> e : deps.entrySet()) {
            String name = e.getKey();
            inDegree.put(name, e.getValue().size());
            for (String d : e.getValue()) {
                if (d.equals(name)) {
                    throw new SchedulerException.InvalidRequest("Task '" + name + "' depends on itself");
                }
                if (!deps.containsKey(d)) {
                    throw new SchedulerException.InvalidRequest(
                            "Task '" + name + "' depends on unknown task '" + d + "'");
                }
                dependents.computeIfAbsent(d, k -> new ArrayList<>()).add(name);
            }
        }

        Deque<String> ready = new ArrayDeque<>();
        for (Map.Entry<String, Integer> e : inDegree.entrySet()) {
            if (e.getValue() == 0) ready.add(e.getKey());
        }

        List<String> order = new ArrayList<>(deps.size());
        while (!ready.isEmpty()) {
            String n = ready.poll();
            order.add(n);
            for (String child : dependents.getOrDefault(n, List.of())) {
                if (inDegree.merge(child, -1, Integer::sum) == 0) {
                    ready.add(child);
                }
            }
        }

        if (order.size() != deps.size()) {
            Set<String> stuck = new TreeSet<>(deps.keySet());
            order.forEach(stuck::remove);
            throw new SchedulerException.InvalidRequest("Dependency cycle detected involving: " + stuck);
        }
        return order;
    }
}
