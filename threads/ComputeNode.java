package csx55.threads;


import csx55.threads.hashing.Task;
import csx55.threads.threadpool.ThreadPool;
import csx55.threads.transport.TCPChannel;
import csx55.threads.transport.TCPServerThread;
import csx55.threads.util.Link;
import csx55.threads.wireformats.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ComputeNode implements Node {

    private final AtomicInteger nodeLoadUpdateCount = new AtomicInteger( 0 );
    private final AtomicInteger numPulled = new AtomicInteger( 0 );
    private final AtomicInteger numPushed = new AtomicInteger( 0 );
    private final AtomicInteger numIsFinishedMessages = new AtomicInteger( 0 );

    private String nextNode;

    private double nodeLoadAverage = 0;
    private int nodeLoadSum = 0;
    private int roundNumber = 0;

    private List<Task> tasks = new ArrayList();
    private List<Task> alreadyMigratedTasks = new ArrayList();


    private TCPChannel registryTcpChannel;
    private ThreadPool threadPool;

    //The ringtopology has information on all links in the system.
    //Specifically, the links track the order
    //because the links themselves in the topology only have one
    //outgoing node, which is the next node in the ring.
    //There's 2 connections for each messaging node,
    //representing backward and forward around the ring,
    //but only one forward link for each link in the ringtopology.
    private RingTopology ringTopology = new RingTopology();

    private Map<String, TCPChannel> tcpChannels = new ConcurrentHashMap<>();
    private Map<String, Integer> nodeLoads = new ConcurrentHashMap<>();

    private final String ipAddress;
    private final int serverSocketPortNumber;
    private String registryIPAddress;
    private int registryPortNumber;


    public ComputeNode(String ipAddress, int serverSocketPortNumber) {
        this.ipAddress = ipAddress;
        this.serverSocketPortNumber = serverSocketPortNumber;
    }

    public ComputeNode(String ipAddress, int serverSocketPortNumber, String registryIPAddress, int registryPortNumber) {
        this.ipAddress = ipAddress;
        this.serverSocketPortNumber = serverSocketPortNumber;
        this.registryIPAddress = registryIPAddress;
        this.registryPortNumber = registryPortNumber;
    }

    public String getIpAddress() {
        return this.ipAddress;
    }

    public int getServerSocketPortNumber() {
        return this.serverSocketPortNumber;
    }

    public static void main(String[] args) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        if (args.length == 2) {

            try {
                //create a new server socket on messaging node
                ServerSocket serverSocket = new ServerSocket(0);

                ComputeNode computeNode = new ComputeNode(InetAddress.getLocalHost().getHostAddress(), serverSocket.getLocalPort(), args[0], Integer.parseInt(args[1]));
                //todo remove
//                System.out.println("Starting up messaging node on ip address and port number: " + InetAddress.getLocalHost().getHostAddress() + ":" + serverSocket.getLocalPort());

                Thread computeNodeServerThread = new Thread(new TCPServerThread(serverSocket, computeNode));
                computeNodeServerThread.start();

                //let the registry know about this new messaging node
                computeNode.registerWithRegistry();

                while (true) {
                    String input;
                    input = reader.readLine();

                    computeNode.processInput(input);
                }

            } catch (IOException | InterruptedException e) {
                System.out.println("Encountered an issue while setting up ComputeNode");
                System.exit(1);
            }

        } else {
            System.out.println("Please enter exactly two arguments for the messaging node call. Exiting.");
            System.exit(1);
        }

    }

    private void processInput(String input) throws IOException, InterruptedException {
        switch (input.toUpperCase()) {
            case "EXIT-OVERLAY":

                if (tcpChannels.size() != 0) {
                    System.out.println("Cannot exit after the overlay has been set up");
                    break;
                }

                DeregisterRequest deregisterRequest = new DeregisterRequest(this.getIpAddress(), this.getServerSocketPortNumber());
                registryTcpChannel.getTcpSenderThread().sendData(deregisterRequest.getbytes());
                break;

            case "LIST-CONNECTIONS":
                for (String connection : tcpChannels.keySet()) {
                    System.out.println(connection);
                }
                break;

            case "LIST-TOPOLOGY":
                for (Link link : ringTopology.getLinks()) {
                    System.out.println(link.getNodeA() + "---" + link.getNodeB() + "---" + link.getOrderNumber());
                }
                break;

            case "LIST-NODE-LOADS":

                for (Map.Entry<String, Integer> nodeLoad : nodeLoads.entrySet()) {
                    System.out.println(nodeLoad.getKey() + " has " + nodeLoad.getValue() + " tasks.");
                }

                break;

            case "PRINT-QUEUE-SIZE":
                System.out.println(threadPool.getQueueSize());

                break;

            default:
                System.out.println("Unknown command. Re-entering wait period.");
                break;
        }

    }

    @Override
    public void onEvent(Event event, TCPChannel tcpChannel) throws IOException, InterruptedException {
        if (event instanceof DeregisterResponse) {

            DeregisterResponse deregisterResponse = (DeregisterResponse) event;
            if (deregisterResponse.getSuccessOrFailure() == 1) {
                System.out.println("Received successful deregister response from the registry. Terminating this messaging node.");

                Thread.sleep(2000);

                registryTcpChannel.getTcpReceiverThread().getDataInputStream().close();
                registryTcpChannel.getTcpSenderThread().getDataOutputStream().close();
                registryTcpChannel.getSocket().close();

                System.exit(1);
            } else {
                System.out.println("Did not receive a successful deregister response from the registry.");
            }

        } else if (event instanceof MessageLoadBalancing) {
            MessageLoadBalancing messageLoadBalancing = (MessageLoadBalancing) event;

            //if messageLoadBalancing meant for this node, add the tasks to this nodes list
            //otherwise, pass it on
            if (messageLoadBalancing.getDestinationNode().equals(this.toString())) {

                nodeLoads.compute(this.toString(), (node, taskCount) -> taskCount + 5);
                numPulled.getAndAdd(5);
                addAlreadyMigratedTasks(messageLoadBalancing.getTradedTasks());

            } else {

                tcpChannels.get(nextNode).getTcpSenderThread().sendData(messageLoadBalancing.getbytes());
            }

        }  else if (event instanceof NodeLoadUpdateMessage) {
            NodeLoadUpdateMessage nodeLoadUpdateMessage = (NodeLoadUpdateMessage) event;

            //if messageLoadBalancing meant for this node, update the load from the update message
            //otherwise, pass it on
            if (nodeLoadUpdateMessage.getDestinationNode().equals(this.toString())) {
                nodeLoads.put(nodeLoadUpdateMessage.getOriginNode(), nodeLoadUpdateMessage.getCurrentNodeLoad());

                //if it's a completion message, also increment the number of finished messages from other compute nodes
                if (nodeLoadUpdateMessage.isFinishedMessage()) {
                    numIsFinishedMessages.getAndIncrement();
                }

            } else {
                tcpChannels.get(nextNode).getTcpSenderThread().sendData(nodeLoadUpdateMessage.getbytes());
            }

        } else if (event instanceof RingTopology) {
            //store ring topology on every messaging node
            RingTopology ringTopology = (RingTopology) event;
            this.ringTopology = ringTopology;

            nextNode = getNextNodeInRing();
            //go through all future connections in ring topology, and if
            //this node is the first node in a link, make a connection to the second node
            for (Link link : ringTopology.getLinks()) {
                if (link.getNodeA().equals(this.toString())) {
                    ComputeNode nodeB = computeNodeDestring(link.getNodeB());

                    Socket socketToComputeNodeToConnectWith = new Socket(nodeB.getIpAddress(), nodeB.getServerSocketPortNumber());
                    TCPChannel tcpChannelOtherComputeNode = new TCPChannel(socketToComputeNodeToConnectWith, this);

                    Thread receivingThread = new Thread(tcpChannelOtherComputeNode.getTcpReceiverThread());
                    receivingThread.start();

                    Thread sendingThread = new Thread(tcpChannelOtherComputeNode.getTcpSenderThread());
                    sendingThread.start();

                    RegisterRequest registerRequest = new RegisterRequest(this.getIpAddress(), this.getServerSocketPortNumber());
                    tcpChannelOtherComputeNode.getTcpSenderThread().sendData(registerRequest.getbytes());

                    //store the new link to the other node... outgoing
                    tcpChannels.put(nodeB.toString(), tcpChannelOtherComputeNode);
                }
            }

            //create thread pools while setting up the overlay
            threadPool = new ThreadPool(ringTopology.getNumberOfThreadsInThreadPool(), this);
            threadPool.startThreadPool();

            //wait for all the connections to be made on all threads before accessing the tcp channels
            //this mainly stops an automated setup-overlay and immediate "start" from being an issue,
            //this the connections may not be setup before this node tries to send a message over
            Thread.sleep(3000);

        } else if (event instanceof RegisterRequest) {

            //store the new link to the other node... incoming
            RegisterRequest registerRequest = (RegisterRequest) event;
            tcpChannels.put(registerRequest.getIpAddress() + ":" + registerRequest.getPortNumber(), tcpChannel);

            //and respond
            RegisterResponse registerResponse = new RegisterResponse((byte) 1, "Response from other messaging node: registration request to " + this.getIpAddress() + ":" + this.getServerSocketPortNumber() + " successful.");
            tcpChannel.getTcpSenderThread().sendData(registerResponse.getbytes());

        } else if (event instanceof RegisterResponse) {
            RegisterResponse registerResponse = (RegisterResponse) event;

        }  else if (event instanceof TaskInitiate) {
            TaskInitiate taskInitiate = (TaskInitiate) event;

            //check if this messaging node has a node list of other connected messaging nodes
            if (tcpChannels.size() == 0) {
                System.out.println("Please setup the overlay before trying to mine.");

            } else {

                roundNumber++;

                long timestamp = System.currentTimeMillis();
                Date date = new Date(timestamp);
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String dateTime = formatter.format(date);
                //todo remove
//                System.out.println("Starting round: " + roundNumber + " at " + dateTime);


                Random random = new Random();
                int numberOfTasks = 1 + random.nextInt(1000);
                //todo remove
//                System.out.println("Processing " + numberOfTasks + " tasks");
                nodeLoads.put(this.toString(), numberOfTasks);

                for (int j = 0; j < numberOfTasks; j++) {
                    Task task = new Task(this.getIpAddress(), this.getServerSocketPortNumber(), roundNumber, new Random().nextInt());
                    tasks.add(task);
                }

                //for all nodes, send the number of tasks that this node has
                for (Link link : ringTopology.getLinks()) {
                    if (!link.getNodeA().equals(this.toString())) {
                        NodeLoadUpdateMessage nodeLoadUpdateMessage = new NodeLoadUpdateMessage(numberOfTasks, link.getNodeA(), this.toString(), false);
                        tcpChannels.get(nextNode).getTcpSenderThread().sendData(nodeLoadUpdateMessage.getbytes());
                    }
                }

                //todo can change this later to a while loop also with a sleep in it, until it becomes true... that might be cleaner
                //this sleep makes it so all nodeloads are updated though when the node loads are averaged below... make sure not to break this
                Thread.sleep(3000);
//                while (nodeLoads.size() != ringTopology.getNumberOfLinks()) {
//                    Thread.sleep(10);
//                }

                nodeLoadSum = nodeLoads.values().stream()
                        .mapToInt(Integer::intValue).sum();
                nodeLoadAverage = nodeLoads.values().stream()
                        .mapToInt(Integer::intValue)
                        .average()
                        .orElse(0);

                //this sleep stops one node from hurrying and calculating the nodeloads before the other nodes do,
                //and then immediately pushing or pulling, which would change the other nodes' loads on another thread
                //and mess with the sum and average calculations
                Thread.sleep(3000);
                double lowerBound = nodeLoadAverage - nodeLoadAverage * .1;
                double upperBound = nodeLoadAverage + nodeLoadAverage * .1;

                //todo remove
//                System.out.println("There are " + nodeLoadSum + " tasks in the system, with average: " + nodeLoadAverage);

                int i = -1;
                while (true) {
                    i++;

                    boolean allWithinAverage = true;
                    for (int nodeLoad : nodeLoads.values()) {
                        if (!(nodeLoad > lowerBound && nodeLoad < upperBound)) {
                            allWithinAverage = false;
                        }
                    }

                    //if excessive balancing is happening, break
                    if (allWithinAverage || (i == 500)) {
                        //todo remove
//                        System.out.println("All within average, or at round 500, after looping " + i + " times! Stop moving tasks");
                        break;
                    }


                    //if this node has more than another, push, as long as there's still tasks that can push
                    if (nodeLoads.get(this.toString()) > nodeLoadAverage) {
                        if (getTasksSize() > 5) {
                            String minComputeNode = getRandomLessThanAverageComputeNode();

                            nodeLoads.compute(this.toString(), (node, taskCount) -> taskCount - 5);
                            numPushed.getAndAdd(5);
                            List<Task> tasksToTransfer = getTasksToTransfer();

                            MessageLoadBalancing messageLoadBalancing = new MessageLoadBalancing(minComputeNode, tasksToTransfer);
                            tcpChannels.get(nextNode).getTcpSenderThread().sendData(messageLoadBalancing.getbytes());
                        }

                    }

                    nodeLoadUpdateCount.getAndIncrement();
                    if (nodeLoadUpdateCount.get() == 3) {
                        //this ensures this node takes a break from spamming the other nodes
                        Thread.sleep(10);
                        nodeLoadUpdateCount.set(0);
                        for (Link link : ringTopology.getLinks()) {
                            if (!link.getNodeA().equals(this.toString())) {
                                NodeLoadUpdateMessage nodeLoadUpdateMessage = new NodeLoadUpdateMessage(nodeLoads.get(this.toString()), link.getNodeA(), this.toString(), false);
                                tcpChannels.get(nextNode).getTcpSenderThread().sendData(nodeLoadUpdateMessage.getbytes());
                            }
                        }
                    }
                }

                //send out this node's information a final time, and wait for other nodes to finish
                //send a complete messed to all others also
                for (Link link : ringTopology.getLinks()) {
                    if (!link.getNodeA().equals(this.toString())) {
                        NodeLoadUpdateMessage nodeLoadUpdateMessage = new NodeLoadUpdateMessage(nodeLoads.get(this.toString()), link.getNodeA(), this.toString(), true);
                        tcpChannels.get(nextNode).getTcpSenderThread().sendData(nodeLoadUpdateMessage.getbytes());
                    }
                }


                //wait for a completion message here, from all other compute nodes, that they have finished load balancing, before processing and moving on
                while (numIsFinishedMessages.get() != (ringTopology.getLinks().size() - 1)) {
                    Thread.sleep(100);
                }

                for (Task task : tasks) {
                    threadPool.put(task);
                }

                for (Task task : alreadyMigratedTasks) {
                    threadPool.put(task);
                }

                while (threadPool.getQueueSize() != 0) {
                    Thread.sleep(100);
                }

                //after a round is done (all tasks are mined... so the thread pool is empty), send a task complete message to the registry
                TaskComplete taskComplete = new TaskComplete(this.toString(), numberOfTasks, numPulled.get(), numPushed.get(), nodeLoads.get(this.toString()));
                registryTcpChannel.getTcpSenderThread().sendData(taskComplete.getbytes());

                //reset values for next time this node gets a task initiate message
                tasks = new ArrayList();
                alreadyMigratedTasks = new ArrayList();

                nodeLoadUpdateCount.set(0);
                nodeLoadAverage = 0;
                nodeLoadSum = 0;
                numPulled.set(0);
                numPushed.set(0);
                numIsFinishedMessages.set(0);
            }

        } else {
            System.out.println("Received unknown event.");
        }
    }

    @Override
    public void sendTaskToRegistry(Task task) throws IOException, InterruptedException {
        TaskMessageToPrint taskMessageToPrint = new TaskMessageToPrint(task);
        registryTcpChannel.getTcpSenderThread().sendData(taskMessageToPrint.getbytes());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        ComputeNode computeNodeObject = (ComputeNode) object;
        return serverSocketPortNumber == computeNodeObject.serverSocketPortNumber && Objects.equals(ipAddress, computeNodeObject.ipAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipAddress, serverSocketPortNumber);
    }

    @Override
    public String toString() {
        return ipAddress + ":" + serverSocketPortNumber;
    }


    private String getRandomLessThanAverageComputeNode() {

        Random random = new Random();
        Map<String, Integer> lessThanAverage = nodeLoads.entrySet().stream()
                .filter(entry -> entry.getValue() < nodeLoadAverage)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<Map.Entry<String, Integer>> listOfLessThanAverageEntries = new ArrayList<>(lessThanAverage.entrySet());
        Map.Entry<String, Integer> randomEntry = listOfLessThanAverageEntries.get(random.nextInt(listOfLessThanAverageEntries.size()));

        return randomEntry.getKey();
    }

    private String getNextNodeInRing() {
        String nodeB = "";
        for (Link link : ringTopology.getLinks()) {
            if (link.getNodeA().equals(this.toString())) {
                return link.getNodeB();
            }
        }
        return nodeB;
    }

    private void registerWithRegistry() throws IOException, InterruptedException {

        Socket socketToHost = new Socket(this.registryIPAddress, this.registryPortNumber);
        TCPChannel tcpChannel = new TCPChannel(socketToHost, this);

        Thread receivingThread = new Thread(tcpChannel.getTcpReceiverThread());
        receivingThread.start();

        Thread sendingThread = new Thread(tcpChannel.getTcpSenderThread());
        sendingThread.start();

        registryTcpChannel = tcpChannel;

        RegisterRequest registerRequest = new RegisterRequest(this.getIpAddress(), this.getServerSocketPortNumber());
        registryTcpChannel.getTcpSenderThread().sendData(registerRequest.getbytes());
    }

    private ComputeNode computeNodeDestring(String firstValue) {
        String[] ipAddressAndPort = firstValue.split(":");
        String ipAddress = ipAddressAndPort[0];
        int portNumber = Integer.parseInt(ipAddressAndPort[1]);
        return new ComputeNode(ipAddress, portNumber);
    }

    private synchronized List<Task> getTasksToTransfer() {
        List<Task> tasksToTransfer = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            tasksToTransfer.add(tasks.remove(0));
        }
        return tasksToTransfer;
    }

    private synchronized void addAlreadyMigratedTasks(List<Task> tasksToAdd) {
        alreadyMigratedTasks.addAll(tasksToAdd);
    }

    private synchronized int getTasksSize() {
        return tasks.size();
    }

}
