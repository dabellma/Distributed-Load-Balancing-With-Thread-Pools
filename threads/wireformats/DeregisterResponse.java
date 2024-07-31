package csx55.threads.wireformats;

import java.io.*;

public class DeregisterResponse implements Event {

    private int messageType;
    private byte successOrFailure;

    public DeregisterResponse(byte successOrFailure) {
        this.messageType = Protocol.DEREGISTER_RESPONSE.getValue();
        this.successOrFailure = successOrFailure;
    }

    public DeregisterResponse(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        byte successOrFailure = dataInputStream.readByte();

        dataInputStream.close();
        baInputStream.close();

        this.successOrFailure = successOrFailure;
    }

    public byte getSuccessOrFailure() {
        return this.successOrFailure;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.DEREGISTER_RESPONSE.getValue());

        dataOutputStream.writeByte(successOrFailure);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
