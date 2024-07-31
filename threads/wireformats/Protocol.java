package csx55.threads.wireformats;

public enum Protocol {
    REGISTER_REQUEST(0),
    DEREGISTER_REQUEST(1),
    REGISTER_RESPONSE(2),
    DEREGISTER_RESPONSE(3),
    TASK_INITIATE(4),
    RING_TOPOLOGY(5),
    TASK_COMPLETE(6),
    MESSAGE_LOAD_BALANCING(7),
    NODE_LOAD_UPDATE_MESSAGE(8),
    TASK_MESSAGE_TO_PRINT(9);

    private final int value;

    Protocol(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
