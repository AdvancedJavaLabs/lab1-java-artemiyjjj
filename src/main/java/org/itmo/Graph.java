package org.itmo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Graph {
    private final int V;
    private final ArrayList<Integer>[] adjList;
    AtomicBoolean[] visited;

    private final ConcurrentLinkedQueue<List<Integer>> globalQueue;
    private final ConcurrentLinkedQueue<List<Integer>> workerQueues;
    private final ExecutorService executor;
    private final int cpus;
    private volatile CountDownLatch latch;
    private final Lock lock = new ReentrantLock(true);
    private final Condition queueReady = this.lock.newCondition();
    // private final Condition workersDone = lock.newCondition();
    private final AtomicBoolean isFinished = new AtomicBoolean(false);
    private final int INTERPROCESS_BATCH = 50;

    Graph(int vertices) {
        this.V = vertices;
        this.visited = new AtomicBoolean[V];
        adjList = new ArrayList[vertices];
        for (int i = 0; i < V; i++) {
            visited[i] = new AtomicBoolean(false);
        }
        for (int i = 0; i < vertices; ++i) {
            adjList[i] = new ArrayList<>();
        }

        // this.cpus = Runtime.getRuntime().availableProcessors();
        this.cpus = 6;
        this.executor = new ThreadPoolExecutor(
            cpus, cpus, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        this.globalQueue = new ConcurrentLinkedQueue<>();
        this.workerQueues = new ConcurrentLinkedQueue<>();
        // this.levelSemaphore = new Semaphore(cpus, true);
        this.latch = new CountDownLatch(cpus);
    }

    class BFSRunnable implements Runnable {
        private final int workerId;
        private final Graph graph;

        public BFSRunnable(Graph graph, int workerId) {
            this.graph = graph;
            this.workerId = workerId;
        }

        @Override
        public void run() {
            graph.latch.countDown();
            // while for each level of graph
            while (!graph.isFinished.get()) {
                List<Integer> vertices = null;
                try {
                    graph.lock.lock();
                    graph.queueReady.await();
                    vertices = graph.globalQueue.poll();
                } catch (InterruptedException e) {
                    System.out.println("Thread " + this.workerId + " is interrupted");
                    break;
                } finally {
                    graph.lock.unlock();
                }

                List<Integer> newVertices = new ArrayList<>();
                while (vertices != null) {
                   
                    for (Integer vertice : vertices) {
                        for (int n : graph.adjList[vertice]) {
                            if (!visited[n].getAndSet(true)) {
                                newVertices.add(n);
                            }
                            if (INTERPROCESS_BATCH == newVertices.size()) {
                                graph.workerQueues.add(new ArrayList<>(newVertices));
                                newVertices.clear();
                            }
                        }
                    }
                    if (!newVertices.isEmpty()) {
                        graph.workerQueues.add(newVertices);
                    }
                    vertices = graph.globalQueue.poll();
                }
                graph.latch.countDown();
            }
        }
    }

    void addEdge(int src, int dest) {
        if (!adjList[src].contains(dest)) {
            adjList[src].add(dest);
        }
    }

    void parallelBFS(int startVertex) {
        try {
            lock.lock();
            for (int i = 0; i < this.cpus; i++) {
                executor.execute(new BFSRunnable(this, i));
            }
            visited[startVertex].set(true);
            List<Integer> initial = new ArrayList<>();
            for (int v : adjList[startVertex]) {
                visited[v].getAndSet(true);
                initial.add(v);
            }
            this.globalQueue.add(initial);
        } finally {
            lock.unlock();
        }
        // wait for workers to start
        try {
            this.latch.await();
        } catch (InterruptedException e) {
            System.out.println("NOT SYNCED");
        } finally {
            this.latch = new CountDownLatch(this.cpus);
        }

        while (!globalQueue.isEmpty()) {
            lock.lock();
            try {
                queueReady.signalAll();
                System.out.println("Global queue before latch: " + globalQueue.size());
            } finally {
                lock.unlock();
            }

            try {
                latch.await();
                lock.lock();
                globalQueue.addAll(workerQueues);
                workerQueues.clear();
                System.out.println("Global queue after latch: " + globalQueue.size());
                latch = new CountDownLatch(cpus);
            } catch (InterruptedException e) {
                System.out.println("MAIN GOES WRONG!!!"); 
            } finally {
                lock.unlock();
            }
        }
        isFinished.set(true);

        lock.lock();
        try {
            queueReady.signalAll();
        } finally {
            lock.unlock();
        }

        executor.shutdownNow();
    }

    //Generated by ChatGPT
    void bfs(int startVertex) {
        boolean[] visited = new boolean[V];

        LinkedList<Integer> queue = new LinkedList<>();

        visited[startVertex] = true;
        queue.add(startVertex);

        while (!queue.isEmpty()) {
            startVertex = queue.poll();

            for (int n : adjList[startVertex]) {
                if (!visited[n]) {
                    visited[n] = true;
                    queue.add(n);
                }
            }
        }
    }

}
