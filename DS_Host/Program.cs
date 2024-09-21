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
        using (var responder = new RouterSocket())
        {
            responder.Bind("tcp://*:5555");

            queue_connected_clients = [];
            queue_busy_clients = [];
            queue_client_heartbeats = [];
            Console.WriteLine("TCP Server up and running");

            int i = 0;
            while (true) // primary loop to run the master node
            {
                /**
                 * Data is sent through messages.
                 * Messages are composed of one or more frames.
                 * Adopted format is: Frame_1 = action type, Frame 2 to N dependent on action type
                 * 
                 * Here, we first determine the action type
                 */
                NetMQMessage message = responder.ReceiveMultipartMessage();

                var clientIdentity = message[0];
                string commandType = message[2].ConvertToString();
                
                Console.WriteLine("Round {0}", ++i);

                switch (commandType)
                {
                    case "REGISTER":
                        Task.Run(() =>
                        {
                            string clientAddress = message[3].ConvertToString();

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
                                responder.SendMoreFrame(clientIdentity.ToByteArray())
                                        .SendMoreFrameEmpty()
                                        .SendMoreFrame("OK")
                                        .SendFrame(clientRank.ToString());
                            }
                            else
                            {
                                
                                if (String.IsNullOrEmpty(clientAddress))
                                {
                                    responder.SendMoreFrame(clientIdentity.ToByteArray())
                                        .SendMoreFrameEmpty()
                                        .SendMoreFrame("ERR")
                                        .SendFrame("Worker TCP Address not provided");
                                }
                                // check if node attempting to register was already in queue
                                if(queue_connected_clients.Contains(clientAddress))
                                {
                                    // if here, it means client had disconnected within heartbeat duration.
                                    // we're playing fair, so they'll have to start afresh
                                    queue_client_heartbeats.Remove(clientAddress);
                                    queue_busy_clients.Remove(clientAddress);
                                    queue_connected_clients.Remove(clientAddress);

                                    responder.SendMoreFrame(clientIdentity.ToByteArray())
                                        .SendMoreFrameEmpty()
                                        .SendMoreFrame("ERR")
                                        .SendFrame("Connection rejected. Please try again");
                                }
                                // Worker limit reached. Cannot admit new ones.
                                responder.SendMoreFrame(clientIdentity.ToByteArray())
                                        .SendMoreFrameEmpty()
                                        .SendMoreFrame("ERR")
                                        .SendFrame("Server has reached maximum capacity");
                            }
                        });
                        break;
                    case "COMMAND":
                        Task.Run(() =>
                        {
                            string workerAddress = message[3].ConvertToString();
                            string commandToExecute = message[4].ConvertToString();

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
                            // eligible if has rank of a higher numeric value and is not currently working.
                            var eligibleWorkers = queue_connected_clients.Where((clientAddress,  clientRank) => clientRank > workerRank);
                            var availableWorkers = eligibleWorkers.Except(queue_busy_clients);
                        
                            // inform requesting node that their request cannot be handled if no workers found
                            if (!availableWorkers.Any())
                            {
                                responder.SendMoreFrame(clientIdentity.ToByteArray())
                                        .SendMoreFrameEmpty()
                                        .SendMoreFrame(DS_ResultType.ERR.ToString())
                                        .SendFrame("There are no available nodes to handle your request");
                                return;
                            }
                            string selectedWorker = availableWorkers.First().ToString();
                        
                            Console.WriteLine("Connecting to worker . .");
                            using (var requester = new RequestSocket())
                            {
                                requester.Connect(String.Format("tcp://{0}", selectedWorker));

                                requester.SendMoreFrame(DS_CommandType.COMMAND.ToString())
                                        .SendFrame(commandToExecute);
                            
                                queue_busy_clients.Add(selectedWorker);

                                NetMQMessage workerResponse = requester.ReceiveMultipartMessage();
                                string resultType = workerResponse[0].ConvertToString();
                                string resultData = workerResponse[1].ConvertToString();
                            
                                queue_busy_clients.Remove(selectedWorker);
                                Console.WriteLine("Worker responded | {0} : {1} ...", resultType, resultData);

                                responder.SendMoreFrame(clientIdentity.ToByteArray())
                                    .SendMoreFrameEmpty()
                                    .SendMoreFrame(resultType)
                                    .SendFrame(resultData);
                            }
                        });
                        break;
                    case "HEARTBEAT":
                        Task.Run(() => 
                        { 
                            string trackedWorkerAddress = message[3].ConvertToString();

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
                                responder.SendMoreFrame(clientIdentity.ToByteArray())
                                        .SendMoreFrameEmpty()
                                        .SendMoreFrame("OK")
                                        .SendFrame("HEARTBEAT Clocked");
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
                                responder.SendMoreFrame(clientIdentity.ToByteArray())
                                        .SendMoreFrameEmpty()
                                        .SendMoreFrame("OK")
                                        .SendFrame("HEARTBEAT Clocked");
                            }
                        });
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
