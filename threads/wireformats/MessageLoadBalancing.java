package csx55.threads.wireformats;

import csx55.threads.hashing.Task;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MessageLoadBalancing implements Event {

    private int messageType;
    private String destinationNode;
    private List<Task> tradedTasks;

    public MessageLoadBalancing(String destinationNode, List<Task> tradedTasks) {
        this.messageType = Protocol.MESSAGE_LOAD_BALANCING.getValue();
        this.destinationNode = destinationNode;
        this.tradedTasks = tradedTasks;
    }

    public MessageLoadBalancing(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);
        List<Task> tradedTasks = new ArrayList<>();

        int messageType = dataInputStream.readInt();

        int destinationNodeSize = dataInputStream.readInt();
        byte[] destinationNodeBytes = new byte[destinationNodeSize];
        dataInputStream.readFully(destinationNodeBytes);
        String destinationNode = new String(destinationNodeBytes, StandardCharsets.UTF_8);


        for (int i=0; i<5; i++) {
            int ipAddressSize = dataInputStream.readInt();
            byte[] ipAddressBytes = new byte[ipAddressSize];
            dataInputStream.readFully(ipAddressBytes);
            String ipAddress = new String(ipAddressBytes, StandardCharsets.UTF_8);

            int portNumber = dataInputStream.readInt();
            int roundNumber = dataInputStream.readInt();
            int taskPayload = dataInputStream.readInt();
            long timestamp = dataInputStream.readLong();
            long threadId = dataInputStream.readLong();
            int nonce = dataInputStream.readInt();

            Task task = new Task(ipAddress, portNumber, roundNumber, taskPayload, timestamp, threadId, nonce);
            tradedTasks.add(task);
        }

        dataInputStream.close();
        baInputStream.close();

        this.destinationNode = destinationNode;
        this.tradedTasks = tradedTasks;
    }

    public String getDestinationNode() {
        return this.destinationNode;
    }
    public List<Task> getTradedTasks() {
        return this.tradedTasks;
    }


    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.MESSAGE_LOAD_BALANCING.getValue());

        byte[] destinationNodeBytes = destinationNode.getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(destinationNodeBytes.length);
        dataOutputStream.write(destinationNodeBytes);

        for (Task task : tradedTasks) {
            byte[] ipAddressBytes = task.getIp().getBytes(StandardCharsets.UTF_8);
            int byteStringLength = ipAddressBytes.length;
            dataOutputStream.writeInt(byteStringLength);
            dataOutputStream.write(ipAddressBytes);

            dataOutputStream.writeInt(task.getPort());
            dataOutputStream.writeInt(task.getRoundNumber());
            dataOutputStream.writeInt(task.getPayload());

            dataOutputStream.writeLong(task.getTimestamp());
            dataOutputStream.writeLong(task.getThreadId());

            dataOutputStream.writeInt(task.getNonce());
        }

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
