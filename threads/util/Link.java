package csx55.threads.util;

public class Link {

    private String nodeA;
    private String nodeB;
    private int orderNumber;

    public Link(String nodeA, String nodeB, int orderNumber) {
        this.nodeA = nodeA;
        this.nodeB = nodeB;
        this.orderNumber = orderNumber;
    }

    public String getNodeA() {
        return this.nodeA;
    }
    public String getNodeB() {
        return this.nodeB;
    }
    public int getOrderNumber() {
        return this.orderNumber;
    }
}
