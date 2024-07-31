package csx55.threads.wireformats;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class EventFactory {
    private static final EventFactory instance = new EventFactory();

    private EventFactory() {}

    public static EventFactory getInstance() {
        return instance;
    }

    public Event createEvent(byte[] incomingMessage) throws IOException {
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(incomingMessage);
        DataInputStream dataInputStream = new DataInputStream(baInputStream);

        //protocol that I always send what type of message it is to then process it
        int messageType = dataInputStream.readInt();

        if (messageType == Protocol.REGISTER_REQUEST.getValue()) {
            RegisterRequest registerRequest = new RegisterRequest(incomingMessage);
            return registerRequest;
        } else if (messageType == Protocol.DEREGISTER_REQUEST.getValue()) {

            DeregisterRequest deregisterRequest = new DeregisterRequest(incomingMessage);
            return deregisterRequest;
        } else if (messageType == Protocol.REGISTER_RESPONSE.getValue()) {
            RegisterResponse registerResponse = new RegisterResponse(incomingMessage);
            return registerResponse;
        } else if (messageType == Protocol.DEREGISTER_RESPONSE.getValue()) {
            DeregisterResponse deregisterResponse = new DeregisterResponse(incomingMessage);
            return deregisterResponse;
        } else if (messageType == Protocol.MESSAGE_LOAD_BALANCING.getValue()) {
            MessageLoadBalancing messageLoadBalancing = new MessageLoadBalancing(incomingMessage);
            return messageLoadBalancing;
        }  else if (messageType == Protocol.NODE_LOAD_UPDATE_MESSAGE.getValue()) {
            NodeLoadUpdateMessage nodeLoadUpdateMessage = new NodeLoadUpdateMessage(incomingMessage);
            return nodeLoadUpdateMessage;
        }  else if (messageType == Protocol.TASK_INITIATE.getValue()) {
            TaskInitiate taskInitiate = new TaskInitiate(incomingMessage);
            return taskInitiate;
        } else if (messageType == Protocol.RING_TOPOLOGY.getValue()) {
            RingTopology ringTopology = new RingTopology(incomingMessage);
            return ringTopology;
        } else if (messageType == Protocol.TASK_COMPLETE.getValue()) {
            TaskComplete taskComplete = new TaskComplete(incomingMessage);
            return taskComplete;
        } else if (messageType == Protocol.TASK_MESSAGE_TO_PRINT.getValue()) {
            TaskMessageToPrint taskMessageToPrint = new TaskMessageToPrint(incomingMessage);
            return taskMessageToPrint;
        } else {
            System.out.println("Unrecognized event in event factory");

        }

        return null;
    }

}
