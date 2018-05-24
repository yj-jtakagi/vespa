package com.yahoo.graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;

/**
 * Provides graph traversal algorithms in an ad-hoc manner.
 *
 * @author jvenstad / jonmv
 */
public class Traversals {

    private Traversals() { }

    /**
     * Returns an {@link Iterable} over the given nodes with a topological iteration order, per the given child mapper.
     *
     * @param nodes An iterable containing all nodes in the graph.
     * @param children A function mapping a node to its children.
     * @param <T> The node type.
     * @return A topologically ordered iterable over the given nodes.
            */
    public static <T> Iterable<T> topologically(Iterable<T> nodes, Function<T, Iterable<T>> children) {
        return () -> new Iterator<T>() {

            Iterator<T> delegate = topological(nodes, children);

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public T next() {
                T next = delegate.next();
                delegate.remove();
                return next;
            }
        };
    }


    /**
     * Returns an {@link Iterable} over the given nodes with a topological iteration order, per the given child mapper.
     *
     * The {@link Iterator#remove()} method must be called for each returned element in order to proceed to its children.
     *
     * @param nodes An iterable containing all nodes in the graph.
     * @param children A function mapping a node to its children.
     * @param <T> The node type.
     * @return A topologically ordered iterable over the given nodes.
     */
    public static <T> Iterator<T> topological(Iterable<T> nodes, Function<T, Iterable<T>> children) {
        Map<T, Long> indegrees = new HashMap<>();
        for (T node : nodes)
            for (T child : children.apply(node))
                indegrees.merge(child, 1L, Long::sum);

        Deque<T> ready = new ArrayDeque<>();
        for (T node : nodes)
            if ( ! indegrees.containsKey(node))
                ready.add(node);

        return new Iterator<T>() {

            T last = null;

            @Override
            public boolean hasNext() {
                return ! ready.isEmpty();
            }

            @Override
            public T next() {
                return last = ready.remove();
            }

            @Override
            public void remove() {
                if (last == null)
                    throw new IllegalStateException();

                for (T child : children.apply(last))
                    indegrees.compute(child, (__, count) -> {
                        if (--count == 0)
                            ready.add(child);
                        return count;
                    });
            }
        };
    }

    public static <T> Function<T, Iterable<T>> reversed(Collection<T> nodes, Function<T, Iterable<T>> parents) {
        Map<T, List<T>> reverse = new HashMap<>();
        for (T node : nodes)
            reverse.put(node, new ArrayList<>());

        for (T node : nodes)
            for (T parent : parents.apply(node))
                if (reverse.containsKey(parent))
                    reverse.get(parent).add(node);

        return reverse::get;
    }

}
