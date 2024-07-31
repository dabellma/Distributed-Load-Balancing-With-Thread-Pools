package csx55.threads.wireformats;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class RegisterResponse implements Event {

    private int messageType;
    private byte successOrFailure;
    private String additionalInfo;

    public RegisterResponse(byte successOrFailure, String additionalInfo) {
        this.messageType = Protocol.REGISTER_RESPONSE.getValue();
        this.successOrFailure = successOrFailure;
        this.additionalInfo = additionalInfo;
    }

    public RegisterResponse(byte[] incomingByteArray) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingByteArray);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        int messageType = dataInputStream.readInt();

        byte successOrFailure = dataInputStream.readByte();

        int additionalInfoLength = dataInputStream.readInt();
        byte[] additionalInfoBytes = new byte[additionalInfoLength];
        dataInputStream.readFully(additionalInfoBytes);
        String additionalInfo = new String(additionalInfoBytes, StandardCharsets.UTF_8);

        dataInputStream.close();
        baInputStream.close();

        this.successOrFailure = successOrFailure;
        this.additionalInfo = additionalInfo;
    }

    @Override
    public byte[] getbytes() throws IOException {
        byte[] marshalledBytes;
        ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream =
                new DataOutputStream(new BufferedOutputStream(baOutputStream));

        dataOutputStream.writeInt(Protocol.REGISTER_RESPONSE.getValue());

        dataOutputStream.writeByte(successOrFailure);

        byte[] additionalInfoBytes = additionalInfo.getBytes(StandardCharsets.UTF_8);
        int byteStringLength = additionalInfoBytes.length;
        dataOutputStream.writeInt(byteStringLength);
        dataOutputStream.write(additionalInfoBytes);

        dataOutputStream.flush();
        marshalledBytes = baOutputStream.toByteArray();
        baOutputStream.close();
        dataOutputStream.close();
        return marshalledBytes;
    }
}
