package csx55.threads.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class NodeLoadUpdateMessage implements Event {

    private int messageType;
    private int currentNodeLoad;
    private String destinationNode;
    private String originNode;
    private boolean finishedMessage;

    public NodeLoadUpdateMessage(int currentNodeLoad, String destinationNode, String originNode, boolean finishedMessage) {
        this.messageType = Protocol.NODE_LOAD_UPDATE_MESSAGE.getValue();
        this.currentNodeLoad = currentNodeLoad;
        this.destinationNode = destinationNode;
        this.originNode = originNode;
        this.finishedMessage = finishedMessage;
    }

    public NodeLoadUpdateMessage(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        int currentNodeLoad = dataInputStream.readInt();

        int destinationNodeSize = dataInputStream.readInt();
        byte[] destinationNodeBytes = new byte[destinationNodeSize];
        dataInputStream.readFully(destinationNodeBytes);
        String destinationNode = new String(destinationNodeBytes, StandardCharsets.UTF_8);


        int originNodeSize = dataInputStream.readInt();
        byte[] originNodeBytes = new byte[originNodeSize];
        dataInputStream.readFully(originNodeBytes);
        String originNode = new String(originNodeBytes, StandardCharsets.UTF_8);

        boolean finishedMessage = dataInputStream.readBoolean();

        dataInputStream.close();
        baInputStream.close();

        this.currentNodeLoad = currentNodeLoad;
        this.destinationNode = destinationNode;
        this.originNode = originNode;
        this.finishedMessage = finishedMessage;
    }

    public int getCurrentNodeLoad() {
        return this.currentNodeLoad;
    }
    public String getDestinationNode() {
        return this.destinationNode;
    }
    public String getOriginNode() {
        return this.originNode;
    }
    public boolean isFinishedMessage() {
        return this.finishedMessage;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.NODE_LOAD_UPDATE_MESSAGE.getValue());

        dataOutputStream.writeInt(currentNodeLoad);

        byte[] destinationNodeBytes = destinationNode.getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(destinationNodeBytes.length);
        dataOutputStream.write(destinationNodeBytes);

        byte[] originNodeBytes = originNode.getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(originNodeBytes.length);
        dataOutputStream.write(originNodeBytes);

        dataOutputStream.writeBoolean(finishedMessage);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
