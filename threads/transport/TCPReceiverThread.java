package csx55.threads.transport;

import csx55.threads.Node;
import csx55.threads.Registry;
import csx55.threads.wireformats.Event;
import csx55.threads.wireformats.EventFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

public class TCPReceiverThread implements Runnable {

    private Socket socket;
    private DataInputStream dataInputStream;
    private Node node;
    private TCPChannel tcpChannel;

    public TCPReceiverThread(Socket socket, Node node, TCPChannel tcpChannel) throws IOException {
        this.socket = socket;
        this.node = node;
        this.dataInputStream = new DataInputStream(socket.getInputStream());
        this.tcpChannel = tcpChannel;

    }

    public DataInputStream getDataInputStream() {
        return this.dataInputStream;
    }

    @Override
    public void run() {
        while (socket != null) {
            try {

                //for this project, the protocol is to receive the length first
                int msgLength = dataInputStream.readInt();
                byte[] incomingMessage = new byte[msgLength];
                dataInputStream.readFully(incomingMessage, 0, msgLength);

                EventFactory eventFactory = EventFactory.getInstance();

                Event event = eventFactory.createEvent(incomingMessage);
                node.onEvent(event, tcpChannel);

            } catch (IOException | InterruptedException exception) {

                if (node instanceof Registry) {
                    Registry registryInstance = (Registry) node;
                    String computeNodeToRemove = null;
                    for (Map.Entry<String, TCPChannel> computeNode : registryInstance.getTCPChannels().entrySet()) {
                        if (computeNode.getValue().equals(tcpChannel)) {
                            computeNodeToRemove = computeNode.getKey();
                        }
                    }

                    if (computeNodeToRemove != null) {
                        System.out.println("Removing " + computeNodeToRemove);
                        registryInstance.getTCPChannels().remove(computeNodeToRemove);
                    }
                }
                break;
            }
        }
    }
}
