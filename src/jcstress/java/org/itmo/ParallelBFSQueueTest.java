package org.itmo;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.*;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.itmo.*;

@JCStressTest
@Outcome(id = "true", expect = Expect.ACCEPTABLE, desc = "Each vertex is visited exactly once")
@Outcome(id = "false", expect = Expect.FORBIDDEN, desc = "Each vertex might be visited more than once")
@State
public class ParallelBFSQueueTest {
    private static final int VERTICES = 20;

    private final Graph graph;
    private int visitedCount = 0;
    private Queue<Integer> currentLevel = new ConcurrentLinkedQueue<>();
    private Queue<Integer> nextLevel = new ConcurrentLinkedQueue<>();

    public ParallelBFSQueueTest() {
        this.graph = new Graph(VERTICES);
        for (int i = 1; i < VERTICES; i++) {
            this.graph.addEdge(0, i);
        }
        this.currentLevel.add(0);
    }

    @Actor
    public void actor1() {
        Integer vertex;
        while ((vertex = currentLevel.poll()) != null) {
            graph.visited[vertex].getAndSet(true);
            for (int newVertex : graph.getAdjList()[vertex]) {
                nextLevel.add(newVertex);
            }
        }
    }

    @Actor
    public void actor2() {
        Integer vertex;
        while ((vertex = currentLevel.poll()) != null) {
            graph.visited[vertex].getAndSet(true);
            for (int newVertex : graph.getAdjList()[vertex]) {
                nextLevel.add(newVertex);
            }
        }
    }

    @Actor
    public void actor3() {
        Integer vertex;
        while ((vertex = currentLevel.poll()) != null) {
            graph.visited[vertex].getAndSet(true);
            for (int newVertex : graph.getAdjList()[vertex]) {
                nextLevel.add(newVertex);
            }
        }
    }

    @Actor
    public void actor4() {
        Integer vertex;
        while ((vertex = currentLevel.poll()) != null) {
            graph.visited[vertex].getAndSet(true);
            for (int newVertex : graph.getAdjList()[vertex]) {
                nextLevel.add(newVertex);
            }
        }
    }

    @Arbiter
    public void arbiter(L_Result r) {
        for (int i = 0; i < VERTICES; i++) {
            if (this.graph.visited[i].get() == true) {
                this.visitedCount++;
            }
        }
        currentLevel = nextLevel;
        r.r1 = visitedCount == 1;
    }
}
