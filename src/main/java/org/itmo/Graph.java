package org.itmo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class Graph {
    private final int V;
    private final List<Integer>[] adjList;

    private final int threads;
    private final AtomicInteger waitingThreads = new AtomicInteger();
    private final ThreadPoolExecutor pool;
    private final boolean[] visited;
    // Очередь с батчами вершин
    private final Queue<List<Integer>> nodeQueue;
    private final Queue<List<Integer>> newNodeQueue;
    // private final Queue<Integer> levelQueue;
    private final PriorityBlockingQueue<Runnable> tasks;
    // private final Semaphore busySemaphore;

    public boolean[] getVisited() {
        return this.visited;
    }

    
    public Queue<List<Integer>> getNodeQueue() {
        return this.nodeQueue;
    }

    
    public PriorityBlockingQueue<Runnable> getTasks() {
        return this.tasks;
    }

    @SuppressWarnings("unchecked")
    Graph(int vertices) {
        this.V = vertices;
        this.adjList = new ArrayList[vertices];
        for (int i = 0; i < vertices; ++i) {
            adjList[i] = new ArrayList<>();
        }
        this.visited = new boolean[this.V];
        this.nodeQueue = new ConcurrentLinkedQueue <List<Integer>>();
        this.newNodeQueue = new ConcurrentLinkedQueue<>();
        
        this.threads = Runtime.getRuntime().availableProcessors();
        this.waitingThreads.set(0);
        System.out.println("CPUS: " + threads);
        this.tasks = new PriorityBlockingQueue<>(V);
        this.pool = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, tasks, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    void addEdge(int src, int dest) {
        if (!adjList[src].contains(dest)) {
            adjList[src].add(dest);
        }
    }

    class BFSRunnable implements Runnable {
        private List<Integer> vertices;
        private final Graph g;

        public BFSRunnable (Graph g) throws InterruptedException {
            this.g = g;
        }

        @Override
        public void run() {
            List<Integer> nodesToVisit = new ArrayList<>();
            
            while (true) {
                synchronized (g.nodeQueue) {
                    try {
                        while (g.nodeQueue.size() == 0) {
                            g.waitingThreads.incrementAndGet();
                            g.nodeQueue.wait();
                            g.waitingThreads.decrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    this.vertices = g.nodeQueue.poll();
                }
                for (int vertex : this.vertices) {
                    for (int n : g.adjList[vertex]) {
                        g.visited[n] = true;
                        nodesToVisit.add(n);
                    }
                }
                // may split these batches to fixed size arrays to provide liveness
                synchronized (g.newNodeQueue) {
                g.newNodeQueue.add(List.copyOf(nodesToVisit));
                nodesToVisit.clear();
                g.newNodeQueue.notifyAll();
                }
            }
        }
    }

    void parallelBFS(int startVertex) throws InterruptedException {
        for (int i = 0; i < threads; i++) {
            pool.execute(new BFSRunnable(this));
        }
        // int debug = 0;
        visited[startVertex] = true;
        List<Integer> initial = new ArrayList<Integer>();
        initial.add(startVertex);
        synchronized (nodeQueue) {
            nodeQueue.add(initial);
            nodeQueue.notifyAll();
        }
        // Нужен барьер для ограничения работы внутри одного уровня графа
        // 1. Потоки будут добвалять новые ноды в одну очередь, а синхронизирующий
        //  поток дожидается выполнения всех потоков на одном уровне, и добавляет все
        // новые ноды в очередь задачь для потоков
        // 2. Явный барьер, изменение состояния которого потоки будут ждать
        // 3. Экзекьютор имеет приоритетную очередь для выполнения тасок, что даёт
        // гарантию начала исполнения тасок для всех узлов одного уровня прежде начала
        // исполнения тасок для узлов следующего. Убирает необходимость в дополнительных 
        // общих на все потоки коллекций с узлами.

        // while (!nodeQueue.isEmpty() || pool.getActiveCount() != 0) {
        
        while (waitingThreads.get() != threads || !nodeQueue.isEmpty() || !newNodeQueue.isEmpty()) {
            List<Integer> list;
            synchronized (newNodeQueue) {
                try {
                    while (newNodeQueue.isEmpty()) {
                        newNodeQueue.wait();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                list = newNodeQueue.poll();
            }
            synchronized (nodeQueue) {
                nodeQueue.add(list);
                nodeQueue.notifyAll();
            }
        }
        pool.shutdown();

        // Отдаём всю очередь этого уровня на обработку потоку пулов
        // while (!nodeQueue.isEmpty()) {
            // while (!nodeQueue.isEmpty()) {
            //     tasks.add(new BFSRunnable(nodeQueue.poll(), this, visited));
            // }
            // .. Когда очередь окажется пустой, дожидаемся завершения обработки всеми потоками
            // long start = 
            // next step - do not return futures, try concurent queue or smth faster
            // List<Future<List<Integer>>> results = pool.invokeAll(tasks);
            // .. Переходим на новый уровень, обновляя очередь задач для потоков
            // for (Future<List<Integer>> elem : results) {
            //     try {
            //         nodeQueue.addAll(elem.get());
            //     } catch (InterruptedException | ExecutionException e) {
            //         throw new RuntimeException("This shouldn't happen because we get Done futures");
            //     }
            // }
            // queue.addAll(results.stream().map(future -> {
            //     try {
            //         return future.get();
            //     } catch (InterruptedException | ExecutionException e) {
            //         throw new RuntimeException("This shouldn't happen because we get Done futures");
            //     }
            // }).collect(Collectors.toList()));
            // tasks.clear();
        // }
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
