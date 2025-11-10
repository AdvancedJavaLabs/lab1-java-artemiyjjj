package org.itmo;

import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.Buffer;
import java.util.HashSet;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

public class BFSTest {

    @Test
    public void bfsTest() throws IOException {
        int[] sizes = new int[]{10, 100, 1000, 10_000, 10_000, 50_000, 100_000, 1_000_000, 2_000_000};
        int[] connections = new int[]{50, 500, 5000, 50_000, 100_000, 1_000_000, 1_000_000, 10_000_000, 10_000_000};
        Random r = new Random(42);
        try (FileWriter fw = new FileWriter("tmp/results.txt")) {
            for (int i = 0; i < sizes.length; i++) {
                for (int j = 0; j < 3; j++) {
                    System.out.println("--------------------------");
                    System.out.println("Generating graph of size " + sizes[i] + " ...wait");
                    Graph g = new RandomGraphGenerator().generateGraph(r, sizes[i], connections[i]);
                    System.out.println("Generation completed!\nStarting bfs");
                    long serialTime = executeSerialBfsAndGetTime(g);
                    long parallelTime = executeParallelBfsAndGetTime(g);
                    fw.append("Times for " + sizes[i] + " vertices and " + connections[i] + " connections: ");
                    System.out.println("Times for " + sizes[i] + " vertices and " + connections[i] + " connections: ");
                    fw.append("\nSerial: " + serialTime + " ms");
                    System.out.println("\nSerial: " + serialTime + " ms");
                    fw.append("\nParallel: " + parallelTime + " ms");
                    System.out.println("\nParallel: " + parallelTime + " ms");
                    fw.append("\n--------\n");
                }
            }
            fw.flush();
        }
    }


    private long executeSerialBfsAndGetTime(Graph g) {
        long startTime = System.currentTimeMillis();
        g.bfs(0);
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    private long executeParallelBfsAndGetTime(Graph g) {
        long startTime = System.currentTimeMillis();
        try {
            g.parallelBFS(0);
        } catch (InterruptedException e) {
            throw new RuntimeException("Do not interrupt parallel BFS!!!");
        }
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

}
