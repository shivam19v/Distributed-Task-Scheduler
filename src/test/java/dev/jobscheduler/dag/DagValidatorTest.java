package dev.jobscheduler.dag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.jobscheduler.dag.DagValidator.Node;
import dev.jobscheduler.domain.SchedulerException;

class DagValidatorTest {

    @Test
    void returnsTopologicalOrderForDiamond() {
        List<String> order = DagValidator.validate(List.of(
                new Node("d", List.of("b", "c")),
                new Node("b", List.of("a")),
                new Node("c", List.of("a")),
                new Node("a", List.of())));

        assertThat(order).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(order.indexOf("a")).isLessThan(order.indexOf("b"));
        assertThat(order.indexOf("a")).isLessThan(order.indexOf("c"));
        assertThat(order.indexOf("b")).isLessThan(order.indexOf("d"));
        assertThat(order.indexOf("c")).isLessThan(order.indexOf("d"));
    }

    @Test
    void rejectsCycles() {
        assertThatThrownBy(() -> DagValidator.validate(List.of(
                new Node("a", List.of("c")),
                new Node("b", List.of("a")),
                new Node("c", List.of("b")),
                new Node("ok", List.of()))))
                .isInstanceOf(SchedulerException.InvalidRequest.class)
                .hasMessageContaining("cycle")
                .hasMessageContaining("a").hasMessageContaining("b").hasMessageContaining("c");
    }

    @Test
    void rejectsSelfDependency() {
        assertThatThrownBy(() -> DagValidator.validate(List.of(new Node("a", List.of("a")))))
                .isInstanceOf(SchedulerException.InvalidRequest.class)
                .hasMessageContaining("itself");
    }

    @Test
    void rejectsUnknownDependency() {
        assertThatThrownBy(() -> DagValidator.validate(List.of(new Node("a", List.of("ghost")))))
                .isInstanceOf(SchedulerException.InvalidRequest.class)
                .hasMessageContaining("ghost");
    }

    @Test
    void rejectsDuplicateNames() {
        assertThatThrownBy(() -> DagValidator.validate(List.of(new Node("a", null), new Node("a", null))))
                .isInstanceOf(SchedulerException.InvalidRequest.class)
                .hasMessageContaining("Duplicate");
    }
}
