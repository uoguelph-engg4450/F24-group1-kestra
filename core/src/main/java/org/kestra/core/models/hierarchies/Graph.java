package org.kestra.core.models.hierarchies;

import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraphBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("UnstableApiUsage")
public class Graph<T, V> {
    private final MutableValueGraph<T, V> graph;

    public Graph() {
        graph = ValueGraphBuilder.directed().build();
    }

    public Graph<T, V> addNode(T node) {
        if (this.graph.nodes().contains(node)) {
            throw new IllegalArgumentException("Already added node '" + node + "'@" + node.hashCode());
        }

        this.graph.addNode(node);

        return this;
    }

    public Graph<T, V> addEdge(T previous, T next, V value) {
        /*
        if (!this.graph.nodes().contains(previous)) {
            throw new IllegalArgumentException("Unable to find previous node '" + previous + "'@" + previous.hashCode());
        }

        if (!this.graph.nodes().contains(next)) {
            throw new IllegalArgumentException("Unable to find next node '" + next + "'" + next.hashCode());
        }
        */

        this.graph.putEdgeValue(previous, next, value);

        return this;
    }

    public Set<T> nodes() {
        return this.graph.nodes();
    }

    public Set<T> successors(T node) {
        return this.graph.successors(node);
    }

    public Set<T> predecessors(T node) {
        return this.graph.predecessors(node);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public Set<Edge<T, V>> edges() {
        return this.graph.edges()
            .stream()
            .map(ts -> new Edge<>(ts.nodeU(), ts.nodeV(), this.graph.edgeValue(ts).get()))
            .collect(Collectors.toSet());
    }

    @Getter
    @AllArgsConstructor
    public static class Edge<T, V> {
        T source;
        T target;
        V value;
    }
}
