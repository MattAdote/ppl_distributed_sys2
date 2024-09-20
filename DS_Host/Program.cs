using NetMQ;
using NetMQ.Sockets;
using static DS_Artefacts.Constants;

static class Program
{
    /**
     * TCP SERVER - HOST / MASTER NODE
     * 
     * Functions:
     * 1. Register tcp clients (workers)
     * 2. Track registered workers to purge disconnected worker nodes
     * 3. Receive worker node commands and forward to other worker nodes
     * 
     */ 
    public static void Main()
    {
        
        List<string> queue_connected_clients;
        List<string> queue_busy_clients;
        Dictionary<string, long> queue_client_heartbeats;

        const int MAX_NUM_WORKER_NODES = 3; // configurable maximum number of clients (workers)
        const int DISCONNECTED_CLIENT_THRESHOLD_MINUTES = 3;
        
        // Setup ZMQ Socket for TCP connections
        using (var responder = new ResponseSocket())
        {
            responder.Bind("tcp://*:5555");

            queue_connected_clients = [];
            queue_busy_clients = [];
            queue_client_heartbeats = [];
            Console.WriteLine("TCP Server up and running");
            
            while (true) // primary loop to run the master node
            {
                /**
                 * Data is sent through messages.
                 * Messages are composed of one or more frames.
                 * Adopted format is: Frame_1 = action type, Frame 2 to N dependent on action type
                 * 
                 * Here, we first determine the action type
                 */ 
                string? commandType = String.Empty;
                bool loadMoreFrames = true;
                responder.TryReceiveFrameString(out commandType, out loadMoreFrames);
                bool isResponseToWorker = false; // used to detect that there's at least one connected worker node

                switch (commandType)
                {
                    case "REGISTER":
                        string? clientAddress = String.Empty;
                        loadMoreFrames = false;
                        isResponseToWorker = responder.TryReceiveFrameString(out clientAddress, out loadMoreFrames);

                        Console.WriteLine(
                            "Received: CommandType = {0} | ClientAddress = {1}",
                            commandType, clientAddress
                        );

                        // check if queue can accommodate new connects
                        if (queue_connected_clients.Count < MAX_NUM_WORKER_NODES && !String.IsNullOrEmpty(clientAddress))
                        {
                            int clientRank;
                            // check if client already registered. If not, add to queue else exit
                            if (!queue_connected_clients.Contains(clientAddress))
                            {
                                queue_connected_clients.Add(clientAddress);
                                clientRank = queue_connected_clients.IndexOf(clientAddress);
                            }
                            else
                            {
                                clientRank = queue_connected_clients.IndexOf(clientAddress);
                            }

                            // respond with client's rank
                            responder.TrySendFrame("OK", true);
                            responder.TrySendFrame(clientRank.ToString());
                        }
                        else
                        {
                            if (isResponseToWorker) 
                            {
                                responder.TrySendFrame("ERR", true);
                                responder.TrySendFrame("Server has reached maximum capacity");
                            }
                        }
                        break;
                    case "COMMAND":
                        string? workerAddress = String.Empty;
                        loadMoreFrames = true;
                        responder.TryReceiveFrameString(out workerAddress, out loadMoreFrames);

                        string? commandToExecute = String.Empty;
                        loadMoreFrames = false;
                        responder.TryReceiveFrameString(out commandToExecute, out loadMoreFrames);

                        Console.WriteLine(
                            "Received: CommandType = {0} | WorkerAddress = {1} | CMD = {2}",
                            commandType, workerAddress, commandToExecute
                        );
                        // get rank of worker node
                        int workerRank = queue_connected_clients.IndexOf(workerAddress);
                        // register worker if rank has value of -1 i.e. workerAddress not found in queue
                        if (workerRank < 0 && queue_connected_clients.Count < MAX_NUM_WORKER_NODES && !String.IsNullOrEmpty(workerAddress))
                        {
                            queue_connected_clients.Add(workerAddress);
                            workerRank = queue_connected_clients.IndexOf(workerAddress);
                        }

                        // get eligible processing nodes
                        // eligible if has rank of a higher numberic value and is not currently working.
                        var eligibleWorkers = queue_connected_clients.Where((clientAddress,  clientRank) => clientRank > workerRank);
                        var availableWorkers = eligibleWorkers.Except(queue_busy_clients);
                        
                        // inform requesting node that their request cannot be handled if no workers found
                        if (!availableWorkers.Any())
                        {
                            responder.TrySendFrame(DS_ResultType.ERR.ToString(), true);
                            responder.TrySendFrame("There are no available nodes to handle your request");
                            break;
                        }
                        string selectedWorker = availableWorkers.First().ToString();
                        
                        Console.WriteLine("Connecting to worker . .");
                        using (var requester = new RequestSocket())
                        {
                            requester.Connect(String.Format("tcp://{0}", selectedWorker));

                            requester.TrySendFrame(DS_CommandType.COMMAND.ToString(), true);
                            bool commandRelayed = requester.TrySendFrame(commandToExecute);

                            if(!commandRelayed)
                            {
                                break;
                            }
                            queue_busy_clients.Add(selectedWorker);
                            
                            // give worker time to process instruction
                            Thread.Sleep(TimeSpan.FromSeconds(1));

                            string? resultType = String.Empty;
                            string? resultData = String.Empty;
                            loadMoreFrames = true;
                            requester.TryReceiveFrameString(out resultType, out loadMoreFrames);

                            loadMoreFrames = false;
                            bool hasWorkerResponded = requester.TryReceiveFrameString(out resultData, out loadMoreFrames);

                            if (hasWorkerResponded)
                            {
                                queue_busy_clients.Remove(selectedWorker);
                                Console.WriteLine("Worker responded | {0} : {1} ...", resultType, resultData);
                            }
                        }
                        break;
                    case "HEARTBEAT":
                        string? trackedWorkerAddress = String.Empty;
                        loadMoreFrames = false;
                        isResponseToWorker = responder.TryReceiveFrameString(out trackedWorkerAddress, out loadMoreFrames);

                        Console.WriteLine(
                            "Received: CommandType = {0} | ClientAddress = {1}",
                            commandType, trackedWorkerAddress
                        );
                        // check if heartbeat sender is in connected queue.
                        bool isCurrentlyTracked = queue_connected_clients.Contains(trackedWorkerAddress);
                        if (!isCurrentlyTracked)
                        {
                            // ensure that they don't exist in busy queue
                            if (queue_busy_clients.Contains(trackedWorkerAddress))
                            {
                                queue_busy_clients.Remove(trackedWorkerAddress);
                            }

                            // add to connected queue
                            queue_connected_clients.Add(trackedWorkerAddress);

                            // upsert to heartbeat queue
                            if(queue_client_heartbeats.ContainsKey(trackedWorkerAddress))
                            {
                                queue_client_heartbeats[trackedWorkerAddress] = DateTime.UtcNow.ToBinary();
                            }
                            else
                            {
                                queue_client_heartbeats.Add(trackedWorkerAddress, DateTime.UtcNow.ToBinary());
                            }

                            // respond to client
                            responder.TrySendFrame("OK", true);
                            responder.TrySendFrame("HEARTBEAT Clocked");
                        }
                        else
                        {
                            // worker is in connected queue. so update the heartbeat
                            if (queue_client_heartbeats.ContainsKey(trackedWorkerAddress))
                            {
                                queue_client_heartbeats[trackedWorkerAddress] = DateTime.UtcNow.ToBinary();
                            }
                            else
                            {
                                queue_client_heartbeats.Add(trackedWorkerAddress, DateTime.UtcNow.ToBinary());
                            }

                            // respond to client
                            responder.TrySendFrame("OK", true);
                            responder.TrySendFrame("HEARTBEAT Clocked");
                        }
                        break;
                }

                // clean up disconnected clients
                // we retain only those heartbeat entries that have been received within the threshold
                DateTime referenceTime = DateTime.UtcNow;
                queue_client_heartbeats = queue_client_heartbeats
                .Where(heartbeat =>
                {
                    TimeSpan timeSinceLastHeartbeat = referenceTime - DateTime.FromBinary(heartbeat.Value);
                    return timeSinceLastHeartbeat < TimeSpan.FromMinutes(DISCONNECTED_CLIENT_THRESHOLD_MINUTES);
                })
                .ToDictionary(validHeartbeat => validHeartbeat.Key, validHeartbeat => validHeartbeat.Value); ;
            }
        }
    }
}
