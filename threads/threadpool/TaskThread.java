package csx55.threads.threadpool;

import csx55.threads.Node;
import csx55.threads.hashing.Miner;
import csx55.threads.hashing.Task;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.LinkedBlockingQueue;

public class TaskThread implements Runnable {

    private final LinkedBlockingQueue<Task> linkedBlockingTaskQueue;
    private Node node;

    public TaskThread(LinkedBlockingQueue<Task> linkedBlockingTaskQueue, Node node) {
        this.linkedBlockingTaskQueue = linkedBlockingTaskQueue;
        this.node = node;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Task task = linkedBlockingTaskQueue.take();

                Miner miner = Miner.getInstance();
                miner.mine(task);

                node.sendTaskToRegistry(task);
                //todo add back in
                System.out.println(task);

            } catch (InterruptedException | NoSuchAlgorithmException | IOException exception) {
                exception.printStackTrace();
            }
        }
    }
}
