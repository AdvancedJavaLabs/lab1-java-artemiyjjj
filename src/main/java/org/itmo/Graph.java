package org.itmo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;

class Graph {
    private final int V;
    private final List<Integer>[] adjList;

    private final int threads;
    private final ThreadPoolExecutor pool;
    // private final ConcurrentLinkedQueue<Integer> visited;
    private final boolean[] visited;
    // Очередь с батчами вершин
    private final Queue<List<Integer>> nodeQueue;
    // private final Queue<Integer> levelQueue;
    private final PriorityBlockingQueue<Runnable> tasks;
    private final Semaphore busySemaphore;

    // public ConcurrentLinkedQueue<Integer> getVisited() {
    //     return this.visited;
    // }
    public boolean[] getVisited() {
        return this.visited;
    }

    
    public Queue<List<Integer>> getNodQueue() {
        return this.nodeQueue;
    }

    
    public PriorityBlockingQueue<Runnable> getTasks() {
        return this.tasks;
    }

    Graph(int vertices) {
        this.V = vertices;
        this.adjList = new ArrayList[vertices];
        for (int i = 0; i < vertices; ++i) {
            adjList[i] = new ArrayList<>();
        }
        // this.visited = new ConcurrentLinkedQueue<>();
        this.visited = new boolean[this.V];
        this.nodeQueue = new ConcurrentLinkedQueue <List<Integer>>();
        // this.levelQueue = new ConcurrentLinkedQueue<>();
        
        this.threads = Runtime.getRuntime().availableProcessors();
        System.out.println("CPUS: " + threads);
        // Comparator<Runnable> comp = (t1, t2) -> Integer.compare(((BFSRunnable)t1).getLayer(), ((BFSRunnable)t2).getLayer());
        this.tasks = new PriorityBlockingQueue<>(V); // ,comp
        this.pool = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, tasks, new ThreadPoolExecutor.CallerRunsPolicy());
        this.busySemaphore = new Semaphore(threads);
    }

    void addEdge(int src, int dest) {
        if (!adjList[src].contains(dest)) {
            adjList[src].add(dest);
        }
    }

    class BFSRunnable implements Runnable {
        private final List<Integer> vertices;
        private final Graph g;
        // Необходимо для гарантии обработки узлов одного уровня
        // private final Integer layer;

        // public Integer getLayer() {
        //     return this.layer;
        // }

        // might remove leyer
        public BFSRunnable (List<Integer> vertices, Integer layer, Graph g) throws InterruptedException {
            this.vertices = vertices;
            // this.layer = layer;
            this.g = g;
            g.busySemaphore.acquire();
        }

        @Override
        public void run() {
            List<Integer> nodesToVisit = new ArrayList<>();
            for (int vertex : this.vertices) {
                for (int n : g.adjList[vertex]) {
                    visited[n] = true;
                    nodesToVisit.add(n);
                    // g.getVisited().add(n);
                    // g.getTasks().add(new BFSRunnable(n, this.layer + 1, g));
                    // g.pool.execute(new BFSRunnable(n, this.layer + 1, g));
                    // nodeQueue.add(n);
                }
            }
            // may split these batches to fixed size arrays to provide liveness
            g.nodeQueue.add(nodesToVisit); // List.copyOf(nodesToVisit)
            g.busySemaphore.release();
        }
    }

    void parallelBFS(int startVertex) throws InterruptedException {
        // int debug = 0;
        visited[startVertex] = true;
        List<Integer> initial = new ArrayList<Integer>();
        initial.add(startVertex);
        nodeQueue.add(initial);
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
        while (busySemaphore.availablePermits() != threads) { // добавить стркутуру которой потоки будут сигнализировать, что они в работе (конд вар?)
            if (!nodeQueue.isEmpty()) {
                pool.execute(new BFSRunnable(nodeQueue.poll(), 0, this));
            }
            // debug++;
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
