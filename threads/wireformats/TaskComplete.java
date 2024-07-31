package csx55.threads.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class TaskComplete implements Event {

    private int messageType;
    private String computeNode;
    private int numGeneratedTasks;
    private int numPulledTasks;
    private int numPushedTasks;
    private int numTasksCompleted;

    public TaskComplete(String computeNode, int numGeneratedTasks, int numPulledTasks, int numPushedTasks, int numTasksCompleted) {
        this.messageType = Protocol.TASK_COMPLETE.getValue();
        this.computeNode = computeNode;
        this.numGeneratedTasks = numGeneratedTasks;
        this.numPulledTasks = numPulledTasks;
        this.numPushedTasks = numPushedTasks;
        this.numTasksCompleted = numTasksCompleted;
    }

    public TaskComplete(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int computeNodeLength = dataInputStream.readInt();
        byte[] computeNodeBytes = new byte[computeNodeLength];
        dataInputStream.readFully(computeNodeBytes);
        String computeNode = new String(computeNodeBytes, StandardCharsets.UTF_8);

        int numGeneratedTasks = dataInputStream.readInt();
        int numPulledTasks = dataInputStream.readInt();
        int numPushedTasks = dataInputStream.readInt();
        int numTasksCompleted = dataInputStream.readInt();

        dataInputStream.close();
        baInputStream.close();

        this.computeNode = computeNode;
        this.numGeneratedTasks = numGeneratedTasks;
        this.numPulledTasks = numPulledTasks;
        this.numPushedTasks = numPushedTasks;
        this.numTasksCompleted = numTasksCompleted;
    }

    public String getComputeNode() {
        return this.computeNode;
    }
    public int getNumGeneratedTasks() {
        return this.numGeneratedTasks;
    }

    public int getNumPulledTasks() {
        return this.numPulledTasks;
    }
    public int getNumPushedTasks() {
        return this.numPushedTasks;
    }

    public int getNumTasksCompleted() {
        return this.numTasksCompleted;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));


        dataOutputStream.writeInt(Protocol.TASK_COMPLETE.getValue());

        byte[] computeNodeBytes = computeNode.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = computeNodeBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(computeNodeBytes);

        dataOutputStream.writeInt(numGeneratedTasks);
        dataOutputStream.writeInt(numPulledTasks);
        dataOutputStream.writeInt(numPushedTasks);
        dataOutputStream.writeInt(numTasksCompleted);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
