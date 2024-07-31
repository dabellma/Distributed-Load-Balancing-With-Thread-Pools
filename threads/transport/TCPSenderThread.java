package csx55.threads.transport;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPSenderThread implements Runnable {

    private DataOutputStream dataOutputStream;
    private BlockingQueue<byte[]> blockingQueue;

    public TCPSenderThread(Socket socket) throws IOException {
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
        this.blockingQueue = new LinkedBlockingQueue<>();
    }

    public void sendData(byte[] dataToSend) throws InterruptedException {
        blockingQueue.put(dataToSend);
    }

    public DataOutputStream getDataOutputStream() {
        return this.dataOutputStream;
    }

    @Override
    public void run() {
        while (true) {
            try {
                byte[] dataToSend = blockingQueue.take();

                //for this project, the protocol is to send the length first
                int dataLength = dataToSend.length;
                dataOutputStream.writeInt(dataLength);

                dataOutputStream.write(dataToSend, 0, dataLength);
                dataOutputStream.flush();

            } catch (IOException | InterruptedException exception) {
                break;
            }
        }
    }
}
