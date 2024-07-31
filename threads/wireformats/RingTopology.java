package csx55.threads.wireformats;

import csx55.threads.util.Link;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RingTopology implements Event {

    private int messageType;
    private int numberOfLinks;
    private List<Link> links;
    private int numberOfThreadsInThreadPool;

    public RingTopology() {
        this.messageType = Protocol.RING_TOPOLOGY.getValue();
        this.numberOfLinks = 0;
        this.links = new ArrayList<>();
        this.numberOfThreadsInThreadPool = 0;
    }

    public RingTopology(int numberOfLinks, List<Link> links, int numberOfThreadsInThreadPool) {
        this.messageType = Protocol.RING_TOPOLOGY.getValue();
        this.numberOfLinks = numberOfLinks;
        this.links = links;
        this.numberOfThreadsInThreadPool = numberOfThreadsInThreadPool;
    }

    public RingTopology(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);
        List<Link> links = new ArrayList<>();

        int messageType = dataInputStream.readInt();

        int numberOfLinks = dataInputStream.readInt();

        for (int i = 0; i < numberOfLinks; i++) {

            int nodeASize = dataInputStream.readInt();
            byte[] nodeABytes = new byte[nodeASize];
            dataInputStream.readFully(nodeABytes);
            String nodeA = new String(nodeABytes, StandardCharsets.UTF_8);

            int nodeBSize = dataInputStream.readInt();
            byte[] nodeBBytes = new byte[nodeBSize];
            dataInputStream.readFully(nodeBBytes);
            String nodeB = new String(nodeBBytes, StandardCharsets.UTF_8);

            int orderNumber = dataInputStream.readInt();

            links.add(new Link(nodeA, nodeB, orderNumber));
        }

        int numberOfThreadsInThreadPool = dataInputStream.readInt();

        dataInputStream.close();
        baInputStream.close();

        this.numberOfLinks = numberOfLinks;
        this.links = links;
        this.numberOfThreadsInThreadPool = numberOfThreadsInThreadPool;
    }

    public int getNumberOfThreadsInThreadPool() {
        return this.numberOfThreadsInThreadPool;
    }

    public List<Link> getLinks() {
        return this.links;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.RING_TOPOLOGY.getValue());

        dataOutputStream.writeInt(numberOfLinks);

        for (Link link : links) {
            byte[] nodeABytes = link.getNodeA().getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(nodeABytes.length);
            dataOutputStream.write(nodeABytes);

            byte[] nodeBBytes = link.getNodeB().getBytes(StandardCharsets.UTF_8);
            dataOutputStream.writeInt(nodeBBytes.length);
            dataOutputStream.write(nodeBBytes);

            dataOutputStream.writeInt(link.getOrderNumber());
        }

        dataOutputStream.writeInt(numberOfThreadsInThreadPool);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
