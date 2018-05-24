package com.yahoo.graph;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.yahoo.graph.Traversals.topologically;
import static com.yahoo.graph.Traversals.topological;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author jvenstad / jonmv
 */
public class TraversalsTest {

    @Test
    public void emptyGraph() {
        assertFalse(topologically(emptyList(), Collections::singletonList).iterator().hasNext());
    }

    @Test
    public void selfCycle() {
        assertFalse(topologically(singletonList(new Object()), Collections::singletonList).iterator().hasNext());
    }

    @Test
    public void singleChildless() {
        Object single = new Object();
        Iterator<?> iterator = topologically(singletonList(single), __ -> emptyList()).iterator();
        assertEquals(single, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void cycle() {
        Node first = new Node(), second = new Node(first);
        first.children.add(second);
        Iterator<?> iterator = topologically(asList(first, second), Node::children).iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void severalChildren() {
        Node third = new Node(), second = new Node(), first = new Node(second, third);
        Iterator<?> iterator = topologically(asList(first, second, third), Node::children).iterator();
        assertEquals(first, iterator.next());
        assertEquals(second, iterator.next());
        assertEquals(third, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void severalParents() {
        Node third = new Node(), second = new Node(third), first = new Node(third);
        Iterator<?> iterator = topologically(asList(first, second, third), Node::children).iterator();
        assertEquals(first, iterator.next());
        assertEquals(second, iterator.next());
        assertEquals(third, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void someGraph() {
        Node    sixth = new Node(),
                fifth = new Node(sixth),
                fourth = new Node(fifth),
                third = new Node(sixth, fifth, fourth),
                second = new Node(),
                first = new Node(sixth, second);
        Iterator<?> iterator = topologically(asList(first, second, third, fourth, fifth, sixth), Node::children).iterator();
        assertEquals(first, iterator.next());
        assertEquals(third, iterator.next());
        assertEquals(second, iterator.next());
        assertEquals(fourth, iterator.next());
        assertEquals(fifth, iterator.next());
        assertEquals(sixth, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void someGraphWithCycle() {
        Node    sixth = new Node(),
                fifth = new Node(sixth),
                fourth = new Node(fifth),
                third = new Node(sixth, fifth, fourth),
                second = new Node(),
                first = new Node(sixth, second);
        sixth.children.add(first);
        Iterator<?> iterator = topologically(asList(first, second, third, fourth, fifth, sixth), Node::children).iterator();
        assertEquals(third, iterator.next());
        assertEquals(fourth, iterator.next());
        assertEquals(fifth, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void incompleteParent() {
        Node second = new Node(), first = new Node(second);
        Iterator<?> iterator = topological(asList(first, second), Node::children);
        assertEquals(first, iterator.next());
        assertFalse(iterator.hasNext());
        iterator.remove();
        assertEquals(second, iterator.next());
        assertFalse(iterator.hasNext());
    }


    static class Node {
        final List<Node> children;
        Node(Node ... children) { this.children = new ArrayList<>(asList(children)); }
        List<Node> children() { return children; }
    }

}
