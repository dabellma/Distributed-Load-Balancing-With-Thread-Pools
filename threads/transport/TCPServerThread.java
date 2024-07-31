package csx55.threads.transport;

import csx55.threads.Node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServerThread implements Runnable {

    private ServerSocket serverSocket;
    private Node node;

    public TCPServerThread(ServerSocket socket, Node node) throws IOException {
        this.serverSocket = socket;
        this.node = node;
    }

    //a thread ceases to be a thread when it gets out of its run method
    @Override
    public void run() {
        while (serverSocket != null) {
            try {
                //this server socket waits for another node to create a socket to connect,
                //and then this server socket completes the connection by creating this receiving
                //and sending socket for this server
                //on another thread. Note that this receiving /socket/ has the sending node's information.
                Socket socket = serverSocket.accept();
                TCPChannel tcpChannel = new TCPChannel(socket, node);

                Thread newReceiverThread = new Thread(tcpChannel.getTcpReceiverThread());
                newReceiverThread.start();

                Thread newSenderThread = new Thread(tcpChannel.getTcpSenderThread());
                newSenderThread.start();

            } catch (IOException e) {
                System.out.println("Unable to establish connection for a TCPServerThread");
            }
        }
    }

}
