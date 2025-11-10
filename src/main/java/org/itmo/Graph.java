package org.itmo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Graph {
    private final int V;
    private final List<Integer>[] adjList;
    private final int threads;
    private final ThreadPoolExecutor pool;
    private final boolean[] visited;
    private final AtomicInteger[] visitedAtomic; // Атомарные флаги для каждой вершины
    
    // Основные структуры данных
    private final ConcurrentLinkedQueue<List<Integer>> globalQueue;
    private final List<ConcurrentLinkedQueue<List<Integer>>> workerQueues;
    private final Phaser levelPhaser;
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final Lock workLock = new ReentrantLock();
    private final Condition workAvailable = workLock.newCondition();
    private final Condition levelCompleted = workLock.newCondition();
    private volatile boolean terminationSignal = false;
    private volatile int currentLevel = 0;
    private final AtomicInteger pendingAdds = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    public Graph(int vertices) {
        this.V = vertices;
        this.adjList = new ArrayList[vertices];
        this.visitedAtomic = new AtomicInteger[vertices];
        for (int i = 0; i < vertices; ++i) {
            adjList[i] = new ArrayList<>();
            visitedAtomic[i] = new AtomicInteger(0);
        }
        this.visited = new boolean[this.V];
        
        this.threads = Runtime.getRuntime().availableProcessors();
        System.out.println("CPUs: " + threads);
        
        // Инициализация очередей
        this.globalQueue = new ConcurrentLinkedQueue<>();
        this.workerQueues = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            workerQueues.add(new ConcurrentLinkedQueue<>());
        }
        
        this.levelPhaser = new Phaser(1);
        
        this.pool = new ThreadPoolExecutor(
            threads, threads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void addEdge(int src, int dest) {
        if (!adjList[src].contains(dest)) {
            adjList[src].add(dest);
        }
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
            try {
                graph.levelPhaser.register();
                
                while (!terminationSignal) {
                    // Ожидаем начала нового уровня
                    int phase = graph.levelPhaser.arriveAndAwaitAdvance();
                    
                    if (terminationSignal) {
                        break;
                    }
                    
                    // Сообщаем о начале работы
                    graph.activeWorkers.incrementAndGet();
                    
                    // Обрабатываем задачи текущего уровня
                    processLevel();
                    
                    // Сообщаем о завершении работы
                    graph.activeWorkers.decrementAndGet();
                    
                    // Уведомляем о возможном завершении уровня
                    graph.signalLevelCompletion();
                }
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            } finally {
                graph.levelPhaser.arriveAndDeregister();
            }
        }

        private void processLevel() {
            while (true) {
                List<Integer> vertices = getTask();
                
                if (vertices == null) {
                    // Попытка украсть работу
                    vertices = stealWork();
                    if (vertices == null) {
                        // Больше нет работы на этом уровне
                        break;
                    }
                }
                
                processVertices(vertices);
            }
        }

        private List<Integer> getTask() {
            List<Integer> task = workerQueues.get(workerId).poll();
            if (task != null) {
                return task;
            }
            return globalQueue.poll();
        }

        private List<Integer> stealWork() {
            for (int i = 0; i < threads; i++) {
                if (i != workerId) {
                    List<Integer> stolenTask = workerQueues.get(i).poll();
                    if (stolenTask != null) {
                        return stolenTask;
                    }
                }
            }
            return null;
        }

        private void processVertices(List<Integer> vertices) {
            List<Integer> newVertices = new ArrayList<>();
            
            for (int vertex : vertices) {
                for (int neighbor : graph.adjList[vertex]) {
                    // Атомарная проверка и установка с помощью CAS
                    if (graph.visitedAtomic[neighbor].compareAndSet(0, 1)) {
                        synchronized (graph.visited) {
                            graph.visited[neighbor] = true;
                        }
                        newVertices.add(neighbor);
                    }
                }
            }
            
            if (!newVertices.isEmpty()) {
                graph.addToNextLevel(newVertices, workerId);
            }
        }
    }

    private void addToNextLevel(List<Integer> newVertices, int workerId) {
        pendingAdds.incrementAndGet();
        try {
            workLock.lock();
            try {
                int targetWorker = (workerId + newVertices.size()) % threads;
                workerQueues.get(targetWorker).offer(new ArrayList<>(newVertices));
                workAvailable.signalAll();
            } finally {
                workLock.unlock();
            }
        } finally {
            pendingAdds.decrementAndGet();
            signalLevelCompletion();
        }
    }

    private void signalLevelCompletion() {
        workLock.lock();
        try {
            levelCompleted.signalAll();
        } finally {
            workLock.unlock();
        }
    }

    public void parallelBFS(int startVertex) throws InterruptedException {
        
        // Инициализация
        visited[startVertex] = true;
        visitedAtomic[startVertex].set(1);
        List<Integer> initialBatch = Collections.singletonList(startVertex);
        globalQueue.offer(initialBatch);

        for (int i = 0; i < threads; i++) {
            pool.execute(new BFSRunnable(this, i));
        }

        int level = 0;
        
        while (true) {
            
            // Начинаем новый уровень
            int phase = levelPhaser.arriveAndAwaitAdvance();
            
            // Ждем реального завершения уровня
            if (!waitForTrueLevelCompletion()) {
                break;
            }
            
            // Подготавливаем следующий уровень
            if (!prepareNextLevel()) {
                break;
            }
            
            level++;
        }
        
        terminationSignal = true;
        levelPhaser.arriveAndAwaitAdvance();
        
        pool.shutdown();
        if (!pool.awaitTermination(2, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
    }

    private boolean waitForTrueLevelCompletion() throws InterruptedException {
        workLock.lock();
        try {
            long startTime = System.currentTimeMillis();
            final long TIMEOUT_MS = 5000;
            
            while (true) {
                boolean queuesEmpty = globalQueue.isEmpty();
                for (ConcurrentLinkedQueue<List<Integer>> queue : workerQueues) {
                    if (!queue.isEmpty()) {
                        queuesEmpty = false;
                        break;
                    }
                }
                
                boolean noActiveWorkers = activeWorkers.get() == 0;
                boolean noPendingAdds = pendingAdds.get() == 0;
                
                if (queuesEmpty && noActiveWorkers && noPendingAdds) {
                    // Двойная проверка для надежности
                    Thread.sleep(1);
                    boolean stillEmpty = globalQueue.isEmpty();
                    for (ConcurrentLinkedQueue<List<Integer>> queue : workerQueues) {
                        if (!queue.isEmpty()) {
                            stillEmpty = false;
                            break;
                        }
                    }
                    if (stillEmpty && activeWorkers.get() == 0 && pendingAdds.get() == 0) {
                        return true;
                    }
                }
                
                if (System.currentTimeMillis() - startTime > TIMEOUT_MS) {
                    System.err.println("Timeout waiting for level completion");
                    return false;
                }
                
                levelCompleted.await(100, TimeUnit.MILLISECONDS);
            }
        } finally {
            workLock.unlock();
        }
    }

    private boolean prepareNextLevel() {
        workLock.lock();
        try {
            // Собираем все задачи для следующего уровня
            boolean hasWork = !globalQueue.isEmpty();
            
            for (ConcurrentLinkedQueue<List<Integer>> workerQueue : workerQueues) {
                List<Integer> batch;
                while ((batch = workerQueue.poll()) != null) {
                    globalQueue.offer(batch);
                    hasWork = true;
                }
            }
            
            return hasWork;
        } finally {
            workLock.unlock();
        }
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