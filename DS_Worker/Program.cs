using NetMQ;
using NetMQ.Sockets;
using static DS_Artefacts.Constants;
static class Program
{
    /**
     * TCP CLIENT - WORKER NODE
     * 
     * Functions:
     * 1. Listen for instructions from master node and execute
     * 2. Send instructions to master node
     * 
     */
    public static void Main()
    {
        const string WORKER_ADDRESS = "localhost:5557";
        const int HEARTBEAT_SEND_DELAY_MINUTES = 1;
        // Function 1. listen for commands to execute on secondary thread
        Task executeCommandFromServer = Task.Run(static () =>
        {
            using (var worker = new ResponseSocket())
            {
                worker.Bind(String.Format("tcp://{0}", WORKER_ADDRESS));

                // register with master node so that master node can know worker is online
                using (var requester = new RequestSocket())
                {
                    Console.WriteLine("Registering with master. . .");
                    requester.Connect("tcp://localhost:5555");

                    requester.TrySendFrame(DS_CommandType.REGISTER.ToString(), true);
                    requester.TrySendFrame(WORKER_ADDRESS);

                    // give master time to process instruction
                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    string? resultType = String.Empty;
                    string? resultData = String.Empty;
                    bool loadMoreFrames = true;
                    requester.TryReceiveFrameString(out resultType, out loadMoreFrames);

                    loadMoreFrames = false;
                    requester.TryReceiveFrameString(out resultData, out loadMoreFrames);

                    Console.WriteLine("Master responded: Result Type = {0} | Result Data = {1}", resultType, resultData);

                    switch (resultType)
                    {
                        case "OK":
                            Console.WriteLine("TCP Client Worker up and running with rank: {0}", resultData);
                            break;
                        case "ERR":
                            Console.WriteLine("TCP Client Worker failed to start: {0}", resultData);
                            throw new Exception(resultData); // exit because master failed to register us
                        default:
                            Console.WriteLine("Master did not respond quickly enough. Assuming network delay. .");
                            break;
                    }
                }

                bool sendHearbeat = true;
                while (true) // main loop to listen for instructions from master
                {
                    // send heartbeat to master to remind him we're still around
                    if (sendHearbeat) 
                    { 
                        Task heartbeat = Task.Run(async () =>
                        {
                            sendHearbeat = false;
                            await Task.Delay(TimeSpan.FromMinutes(HEARTBEAT_SEND_DELAY_MINUTES));
                        
                            using var requester = new RequestSocket();
                            requester.Connect("tcp://localhost:5555");

                            Console.WriteLine("Sending heartbeat to master. . .");
                            requester.TrySendFrame(DS_CommandType.HEARTBEAT.ToString(), true);
                            requester.TrySendFrame(WORKER_ADDRESS);

                            // give master time to process instruction
                            Thread.Sleep(TimeSpan.FromSeconds(1));

                            string? resultType = String.Empty;
                            string? resultData = String.Empty;
                            bool loadMore = true;
                            requester.TryReceiveFrameString(out resultType, out loadMore);

                            loadMore = false;
                            requester.TryReceiveFrameString(out resultData, out loadMore);

                            Console.WriteLine("Master responded: Result Type = {0} | Result Data = {1}", resultType, resultData);

                            switch (resultType)
                            {
                                case "OK":
                                    Console.WriteLine("Master confirms heartbeat");
                                    break;
                                case "ERR":
                                    Console.WriteLine("Master provided error response: {0}", resultData);
                                    break;
                            }

                        });
                        heartbeat.Wait();
                        sendHearbeat = true;
                    }
                    
                    // check for commands from master
                    string? commandType = String.Empty;
                    string? cmdToExecute = String.Empty;
                    bool loadMoreFrames = true;
                    worker.TryReceiveFrameString(out commandType, out loadMoreFrames);

                    loadMoreFrames = false;
                    bool isCommandFromMaster = worker.TryReceiveFrameString(out cmdToExecute, out loadMoreFrames);

                    if(isCommandFromMaster)
                    {
                        Console.WriteLine("Received {0}", cmdToExecute);
                        worker.TrySendFrame(DS_ResultType.OK.ToString(), true);
                        worker.TrySendFrame(String.Format("Successfully processed instruction: {0}", cmdToExecute));
                    }
                }
            }
        });

        // Function 2. send commands to master
        bool endApp = false;
        while (!endApp)
        {
            Console.WriteLine("Enter command to issue");
            string? cmdToExecute = Console.ReadLine();

            Console.WriteLine("Connecting to master . .");
            using var requester = new RequestSocket();
            requester.Connect("tcp://localhost:5555");

            requester.TrySendFrame(DS_CommandType.COMMAND.ToString(), true);
            requester.TrySendFrame(WORKER_ADDRESS, true);
            requester.TrySendFrame(cmdToExecute ?? String.Empty);

            // give master time to process instruction
            Thread.Sleep(TimeSpan.FromSeconds(1));

            string? resultType = String.Empty;
            string? resultData = String.Empty;
            bool loadMoreFrames = true;
            requester.TryReceiveFrameString(out resultType, out loadMoreFrames);

            loadMoreFrames = false;
            bool hasMasterResponded = requester.TryReceiveFrameString(out resultData, out loadMoreFrames);

            if (hasMasterResponded)
            {
                Console.WriteLine("Master responded {0} : {1} ...", resultType, resultData);
            }
            // Prompt user whether to exit or hold on.
            Console.WriteLine("Press 'x' and Enter to close the app, or press any other key and Enter to continue: ");
            if (Console.ReadLine() == "x") endApp = true;
        }
    }
}
