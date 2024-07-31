package csx55.threads;


import csx55.threads.hashing.Task;
import csx55.threads.transport.TCPChannel;
import csx55.threads.wireformats.Event;

import java.io.IOException;

public interface Node {
    void onEvent(Event event, TCPChannel tcpChannel) throws IOException, InterruptedException;
    void sendTaskToRegistry(Task task) throws IOException, InterruptedException;
}
