Miner - mines a task to constitute a proof of work
Task - unit of work similar to how mining is performed in some cryptocurrencies
TaskThread - thread that is created as part of the thread pool, with a linked blocking queue
ThreadPool - holds 2-16 threads, defined by user
TCPChannel - describes a connection between 2 nodes... registry -> compute node, or compute node -> compute node
TCPReceiverThread - thread to receive communications on a node
TCPSenderThread - thread to send communications on a node
TCPServerThread - thread to accept connections
DeregisterRequest - compute node can deregister from registry
DeregisterResponse - message format sent from registry to compute node on deregistration
Event - interface for defining messages
EventFactory - processes incoming messages
MessageLoadBalancing - used to send/receive tasks
NodeLoadUpdateMessage - heartbeat message describing number of tasks a node has; also used when a compute node has finished its load balancing and tells other nodes it's ready to process tasks
Protocol - enum for the message types
RegisterRequest - compute node to register with registry or another compute node (sets up a connection)
RegisterResponse - response to a registration request
RingTopology - provided to the compute nodes by the registry to tell them who to connect to (to create a ring) and how many threads to put in their thread pools
TaskComplete - sent by each compute node after they finish processing their tasks for a round
TaskInitiate - sent by registry to each compute node for each round, to create tasks, load balance them among the compute nodes, and mine them
TaskMessageToPrint - used by a compute node to message the registry and tell the registry about the compute node's completed tasks
ComputeNode - part of a ring topology/overlay that contains a thread pool, and load balances tasks in the system before processing those tasks
Node - interface for common methods between compute nodes and the registry
Registry - initiator of the task creation, balancing, and mining process for the compute nodes

For load balancing, nodes that have more than the average number of tasks choose a node that has less than the average number of tasks and give them 5 tasks at a time.
Communications are done in a ring.