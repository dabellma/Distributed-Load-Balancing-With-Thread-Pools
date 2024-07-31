package csx55.threads;


import csx55.threads.hashing.Task;
import csx55.threads.transport.TCPChannel;
import csx55.threads.transport.TCPServerThread;
import csx55.threads.util.Link;
import csx55.threads.wireformats.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Registry implements Node {

    private AtomicInteger taskCompleteMessagesReceived = new AtomicInteger(0);
    private AtomicInteger minedTasks = new AtomicInteger(0);

    private Map<String, TCPChannel> tcpChannels = new ConcurrentHashMap<>();
    private RingTopology ringTopology = new RingTopology();
    private Map<String, TaskComplete> taskCompleteMessages = new ConcurrentHashMap<>();
    private int threadPoolSize = 0;
    private int numberOfRounds = 0;

    public Map<String, TCPChannel> getTCPChannels() {
        return this.tcpChannels;
    }

    public static void main(String[] args) {

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        if (args.length == 1) {
            try {
                int portNumber = Integer.parseInt(args[0]);

                //todo remove
//                System.out.println("Starting up registry on ip address and port number: " + InetAddress.getLocalHost().getHostAddress() + " " + portNumber);

                Registry registry = new Registry();

                //Create the socket to accept incoming messages from messenger nodes
                ServerSocket ourServerSocket = new ServerSocket(Integer.parseInt(args[0]));

                //Spawn a thread to receive connections to the Registry node
                //while at the same time being able to run as a foreground process
                Thread registryTCPServerThread = new Thread(new TCPServerThread(ourServerSocket, registry));
                registryTCPServerThread.start();

                while (true) {
                    String input;
                    input = reader.readLine();

                    registry.processInput(input);
                }

            } catch (IOException | InterruptedException exception) {
                System.out.println("Encountered an issue while setting up main Registry");
                System.exit(1);
            }

        } else {
            System.out.println("Please enter exactly one argument. Exiting.");
            System.exit(1);
        }
    }

    private void processInput(String input) throws IOException, InterruptedException {
        String[] tokens = input.split("\\s+");
        switch (tokens[0].toUpperCase()) {

            case "LIST-MESSAGING-NODES":
                for (String connection : tcpChannels.keySet()) {
                    System.out.println(connection);
                }
                break;

            case "LIST-TOPOLOGY":
                for (Link link : ringTopology.getLinks()) {
                    System.out.println(link.getNodeA() + "---" + link.getNodeB() + "---" + link.getOrderNumber());
                }
                break;

            case "PRINT-NUMBER-OF-MINED-TASKS":
                System.out.println(minedTasks.get());
                break;

            case "PRINT-SUMMARY":
                printSummary();
                break;

            case "SETUP-OVERLAY":

                if (threadPoolSize != 0 ) {
                    System.out.println("Already set up the ring topology and thread pools");
                    break;
                }

                if (tokens.length != 2) {
                    System.out.println("Please provide a thread pool size when setting up the overlay.");
                    break;
                }

                threadPoolSize = Integer.parseInt(tokens[1]);
                if (threadPoolSize < 2 || threadPoolSize > 16) {
                    System.out.println("Thread pool should have between 2 and 16 threads.");
                    break;
                }

                List<Link> links = new ArrayList<>();

                for (int i=0; i < tcpChannels.size(); i++) {

                    String fromComputeNode;
                    String toComputeNode;

                    //if last node in ring
                    if (i == (tcpChannels.size() - 1)) {
                        fromComputeNode = getComputeNodeFromSet(i);
                        toComputeNode = getComputeNodeFromSet(0);
                    //if not last node in ring
                    } else {
                        fromComputeNode = getComputeNodeFromSet(i);
                        toComputeNode = getComputeNodeFromSet(i+1);
                    }

                    links.add(new Link(fromComputeNode, toComputeNode, i));

                }

                //send the list of links and ordering to all messaging nodes
                ringTopology = new RingTopology(links.size(), links, threadPoolSize);
                for (String computeNode : tcpChannels.keySet()) {

                    TCPChannel tcpChannel = tcpChannels.get(computeNode);
                    tcpChannel.getTcpSenderThread().sendData(ringTopology.getbytes());

                }

                //for each node, create a task complete entry in the task completion map
                for (String computeNode : tcpChannels.keySet()) {
                    taskCompleteMessages.put(computeNode, new TaskComplete(computeNode, 0, 0, 0, 0));
                }

                break;

            case "START":

                if (tokens.length != 2) {
                    System.out.println("Please add a number of rounds.");
                    break;
                }

                if (ringTopology.getLinks().size() != tcpChannels.size()) {
                    System.out.println("Please set up ring topology first.");
                    break;
                }

                numberOfRounds = Integer.parseInt(tokens[1]);

                for (int i=0; i < numberOfRounds; i++) {
                    int currentRound = i + 1;
                    //todo remove
//                    System.out.println("Starting round number: " + currentRound);
                    for (String computeNode : tcpChannels.keySet()) {

                        TaskInitiate taskInitiate = new TaskInitiate();

                        //get socket to a registered node
                        TCPChannel tcpChannel = tcpChannels.get(computeNode);
                        tcpChannel.getTcpSenderThread().sendData(taskInitiate.getbytes());
                    }

                    //check for all messages from all nodes to be received
                    //before starting the next round for all compute nodes
                    while (taskCompleteMessagesReceived.get() != tcpChannels.size()) {
                        Thread.sleep(100);
                    }


                    //set back to 0 for next round
                    taskCompleteMessagesReceived.set(0);
                }

                printSummary();
                break;

            default:
                System.out.println("Unknown command. Re-entering wait period.");
                break;
        }
    }

    @Override
    public void onEvent(Event event, TCPChannel tcpChannel) throws IOException, InterruptedException {
        if (event instanceof DeregisterRequest) {
            DeregisterRequest deregisterRequest = (DeregisterRequest) event;

            if (!deregisterRequest.getIpAddress().equals(tcpChannel.getSocket().getInetAddress().getHostAddress())) {
                System.out.println("Did not pass checks to deregister this messaging node because there's a mismatch between the deregister address and the address of the request");

                //send a response that the messaging node can not safely exit and terminate the process
                DeregisterResponse deregisterResponse = new DeregisterResponse((byte) 0);
                tcpChannel.getTcpSenderThread().sendData(deregisterResponse.getbytes());
            } else if (!tcpChannels.containsKey(new ComputeNode(deregisterRequest.getIpAddress(), deregisterRequest.getPortNumber()).toString())) {
                System.out.println("Did not pass checks to deregister this messaging node because the deregister address doesn't exist in the registry");

                //send a response that the messaging node can not safely exit and terminate the process
                DeregisterResponse deregisterResponse = new DeregisterResponse((byte) 0);
                tcpChannel.getTcpSenderThread().sendData(deregisterResponse.getbytes());
            } else {
                //if there's a successful deregistration
                System.out.println("Deregistering a messaging node with the registry");
                String computeNodeToRemove = new ComputeNode(deregisterRequest.getIpAddress(), deregisterRequest.getPortNumber()).toString();

                tcpChannels.remove(computeNodeToRemove);

                //send a response that the messaging node can safely exit and terminate the process
                DeregisterResponse deregisterResponse = new DeregisterResponse((byte) 1);
                tcpChannel.getTcpSenderThread().sendData(deregisterResponse.getbytes());

                Thread.sleep(2000);

                tcpChannel.getTcpReceiverThread().getDataInputStream().close();
                tcpChannel.getTcpSenderThread().getDataOutputStream().close();
                tcpChannel.getSocket().close();
            }

        } else if (event instanceof RegisterRequest) {
            RegisterRequest registerRequest = (RegisterRequest) event;

            //if there's an unsuccessful registration
            if (!registerRequest.getIpAddress().equals(tcpChannel.getSocket().getInetAddress().getHostAddress())) {

                System.out.println("Did not pass checks to register this messaging node because there's a mismatch between the registration address and the address of the request" );

                //send a response from registry to messaging node
                //saying the messaging node did not register successfully
                RegisterResponse registerResponse = new RegisterResponse((byte) 0, "Registration request unsuccessful because there's a mismatch between the registration address and the address of the request");
                tcpChannel.getTcpSenderThread().sendData(registerResponse.getbytes());

            } else if (tcpChannels.containsKey(new ComputeNode(registerRequest.getIpAddress(), registerRequest.getPortNumber()).toString())) {
                System.out.println("Did not pass checks to register this messaging node because this node already exists in the registry" );

                //send a response from registry to messaging node
                //saying the messaging node did not register successfully
                RegisterResponse registerResponse = new RegisterResponse((byte) 0, "Registration request unsuccessful because this node already exists in the registry");
                tcpChannel.getTcpSenderThread().sendData(registerResponse.getbytes());
            } else {
                //if there's a successful registration
                String computeNode = new ComputeNode(registerRequest.getIpAddress(), registerRequest.getPortNumber()).toString();
                tcpChannels.put(computeNode, tcpChannel);

                //send a response from registry to messaging node saying the messaging node registered successfully
                RegisterResponse registerResponse = new RegisterResponse((byte) 1, "Registration request to registry was successful. The number of messaging nodes currently constituting the overlay is " + tcpChannels.size());
                tcpChannel.getTcpSenderThread().sendData(registerResponse.getbytes());
            }

        } else if (event instanceof TaskComplete) {
            TaskComplete taskComplete = (TaskComplete) event;
            TaskComplete taskCompleteCurrent = taskCompleteMessages.get(taskComplete.getComputeNode());
            TaskComplete taskCompleteNewEntry = new TaskComplete(taskComplete.getComputeNode(), taskCompleteCurrent.getNumGeneratedTasks() + taskComplete.getNumGeneratedTasks(),
                    taskCompleteCurrent.getNumPulledTasks() + taskComplete.getNumPulledTasks(),
                    taskCompleteCurrent.getNumPushedTasks() + taskComplete.getNumPushedTasks(),
                    taskCompleteCurrent.getNumTasksCompleted() + taskComplete.getNumTasksCompleted());

            taskCompleteMessages.put(taskComplete.getComputeNode(), taskCompleteNewEntry);
            taskCompleteMessagesReceived.getAndIncrement();

        } else if (event instanceof TaskMessageToPrint) {
            TaskMessageToPrint taskMessageToPrint = (TaskMessageToPrint) event;
            minedTasks.getAndIncrement();

        } else {
            System.out.println("Received unknown event");
        }
    }

    @Override
    public void sendTaskToRegistry(Task task) {
        System.out.println("This is here for ease, since Registry and ComputeNode both implement Node.");
    }

    private String getComputeNodeFromSet(int numberOfItemInMap) {
        if (numberOfItemInMap > tcpChannels.size()){
            throw new RuntimeException();
        }
        int number = 0; //start with a number of 0 for the first item in the set
        for (String computeNode : tcpChannels.keySet()) {
            if (number == numberOfItemInMap) {
                return computeNode;
            }
            number++;
        }
        return null;
    }


    private void printSummary() {
        for (int i=0; i < 10; i++) {
//            System.out.println();
        }

        String topRowFormat = "%-20s | %-20s | %-20s | %-20s | %-20s | %-20s%n";
        String format = "%-20s | %-20s | %-20s | %-20s | %-20s | %-20s%n";
        String totalFormat = "%-20s | %-20s | %-20s | %-20s | %-20s | %-20s%n";


        int totalNumGenerated = 0;
        int totalNumPushed = 0;
        long totalNumPulled = 0;
        long totalNumCompleted = 0;
        double totalPercentPerformed = 0;


        for (Map.Entry<String, TaskComplete> taskCompleteEntry : taskCompleteMessages.entrySet()) {
            totalNumGenerated += taskCompleteEntry.getValue().getNumGeneratedTasks();

        }

        System.out.printf(topRowFormat, "", "Num generated tasks", "Num pulled tasks", "Num pushed tasks", "Num completed tasks", "Percent of total tasks performed");

        for (Map.Entry<String, TaskComplete> taskCompleteEntry : taskCompleteMessages.entrySet()) {
            double percentPerformedThisNode = (double) taskCompleteEntry.getValue().getNumTasksCompleted() / totalNumGenerated;
            System.out.printf(format, taskCompleteEntry.getKey(), taskCompleteEntry.getValue().getNumGeneratedTasks(), taskCompleteEntry.getValue().getNumPulledTasks(),
                    taskCompleteEntry.getValue().getNumPushedTasks(), taskCompleteEntry.getValue().getNumTasksCompleted(), percentPerformedThisNode * 100);
            totalNumPushed += taskCompleteEntry.getValue().getNumPulledTasks();
            totalNumPulled += taskCompleteEntry.getValue().getNumPushedTasks();
            totalNumCompleted += taskCompleteEntry.getValue().getNumTasksCompleted();
            totalPercentPerformed += (double) taskCompleteEntry.getValue().getNumTasksCompleted() / totalNumGenerated;
        }

        int totalPercentPerformedRounded = (int) Math.round(totalPercentPerformed * 100);
//        System.out.println();
        System.out.printf(totalFormat, "Total", totalNumGenerated, totalNumPushed, totalNumPulled, totalNumCompleted, totalPercentPerformedRounded);

//        System.out.println();
//        System.out.println("For " + numberOfRounds + " rounds, with " + tcpChannels.size() + " compute nodes, each with a thread pool of size " + threadPoolSize + " threads.");

        for (int i=0; i < 10; i++) {
//            System.out.println();
        }
    }

}
