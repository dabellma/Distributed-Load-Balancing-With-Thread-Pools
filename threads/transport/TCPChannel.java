package csx55.threads.transport;

import csx55.threads.Node;

import java.io.IOException;
import java.net.Socket;

public class TCPChannel {

    private Socket socket;
    private TCPReceiverThread tcpReceiverThread;
    private TCPSenderThread tcpSenderThread;

    public TCPChannel(Socket socket, Node node) throws IOException {
        this.socket = socket;
        this.tcpReceiverThread = new TCPReceiverThread(socket, node, this);
        this.tcpSenderThread = new TCPSenderThread(socket);
    }

    public Socket getSocket() {
        return this.socket;
    }

    public TCPReceiverThread getTcpReceiverThread() {
        return this.tcpReceiverThread;
    }

    public TCPSenderThread getTcpSenderThread() {
        return this.tcpSenderThread;
    }
}
