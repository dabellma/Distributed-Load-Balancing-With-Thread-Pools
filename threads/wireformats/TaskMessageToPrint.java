package csx55.threads.wireformats;

import csx55.threads.hashing.Task;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class TaskMessageToPrint implements Event {


    private int messageType;
    private Task task;

    public TaskMessageToPrint(Task task) {
        this.messageType = Protocol.TASK_MESSAGE_TO_PRINT.getValue();
        this.task = task;
    }

    public TaskMessageToPrint(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int ipAddressSize = dataInputStream.readInt();
        byte[] ipAddressBytes = new byte[ipAddressSize];
        dataInputStream.readFully(ipAddressBytes);
        String ipAddress = new String(ipAddressBytes, StandardCharsets.UTF_8);

        int portNumber = dataInputStream.readInt();
        int roundNumber = dataInputStream.readInt();
        int payload = dataInputStream.readInt();
        long timestamp = dataInputStream.readLong();
        long threadId = dataInputStream.readLong();
        int nonce = dataInputStream.readInt();

        Task task = new Task(ipAddress, portNumber, roundNumber, payload, timestamp, threadId, nonce);

        dataInputStream.close();
        baInputStream.close();

        this.task = task;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.TASK_MESSAGE_TO_PRINT.getValue());

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

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
