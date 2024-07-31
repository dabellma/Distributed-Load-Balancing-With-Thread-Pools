package csx55.threads.threadpool;

import csx55.threads.Node;
import csx55.threads.hashing.Task;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPool {

    private final LinkedBlockingQueue<Task> linkedBlockingTaskQueue;
    private final Set<Thread> threadsInThreadPool;
    private Node node;

    public ThreadPool(int numberOfThreadsInThreadPool, Node node) {
        this.linkedBlockingTaskQueue = new LinkedBlockingQueue<>();
        this.threadsInThreadPool = new HashSet<>(numberOfThreadsInThreadPool);
        this.node = node;

        for (int i=0; i < numberOfThreadsInThreadPool; i++) {
            Thread thread = new Thread(new TaskThread(linkedBlockingTaskQueue, node));
            threadsInThreadPool.add(thread);

        }
    }

    public void startThreadPool() {
        for (Thread thread : threadsInThreadPool) {
            thread.start();
        }
    }

    public void put(Task task) throws InterruptedException {
        linkedBlockingTaskQueue.put(task);
    }

    public int getQueueSize() {
        return linkedBlockingTaskQueue.size();
    }
}
