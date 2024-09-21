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
     * 3. Send heartbeat to master to signal we're still alive
     */
    public static void Main()
    {
        const string MASTER_ADDRESS = "localhost:5555";
        const string WORKER_ADDRESS = "localhost:5556";
        const int HEARTBEAT_SEND_DELAY_MINUTES = 1;

        // register with master node so that master can know worker is online.
        // we do not proceed at all if master fails to acknowledge us
        using (var requester = new RequestSocket())
        {
            Console.WriteLine("Registering with master. . .");
            requester.Connect(String.Format("tcp://{0}", MASTER_ADDRESS));

            requester.TrySendFrame(DS_CommandType.REGISTER.ToString(), true);
            requester.TrySendFrame(WORKER_ADDRESS);

            NetMQMessage masterResponse = requester.ReceiveMultipartMessage();

            string resultType = masterResponse[0].ConvertToString();
            string resultData = masterResponse[1].ConvertToString();

            switch (resultType)
            {
                case "OK":
                    Console.WriteLine("TCP Client Worker up and running with rank: {0}", resultData);
                    break;
                case "ERR":
                    Console.WriteLine("TCP Client Worker Registration failed: {0}", resultData);
                    throw new Exception(resultData); // exit because master failed to register us
            }
        }

        // Function 1. listen for commands to execute on secondary thread
        Task executeCommandFromServer = Task.Run(static () =>
        {
            using (var worker = new ResponseSocket())
            {
                worker.Bind(String.Format("tcp://{0}", WORKER_ADDRESS));
                
                while (true) // main loop to listen for instructions from master
                {
                    // check for commands from master
                    NetMQMessage message = worker.ReceiveMultipartMessage();
                    string commandType = message[0].ConvertToString();
                    string cmdToExecute = message[1].ConvertToString();
                    
                    Console.WriteLine("Received {0}", cmdToExecute);
                    
                    // issue response to master
                    worker.SendMoreFrame(DS_ResultType.OK.ToString())
                        .SendFrame(String.Format("Successfully processed instruction: {0}", cmdToExecute));
                }
            }
        });

        // Function 3. Send hearbeat to master every so often as configured
        Task sendHeartbeat = Task.Run(static async () =>
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromMinutes(HEARTBEAT_SEND_DELAY_MINUTES));

                using var requester = new RequestSocket();
                requester.Connect(String.Format("tcp://{0}", MASTER_ADDRESS));

                Console.WriteLine("Sending heartbeat to master. . .");
                requester.SendMoreFrame(DS_CommandType.HEARTBEAT.ToString())
                        .SendFrame(WORKER_ADDRESS);

                NetMQMessage message = requester.ReceiveMultipartMessage();
                string resultType = message[0].ConvertToString();
                string resultData = message[1].ConvertToString();

                switch (resultType)
                {
                    case "OK":
                        Console.WriteLine("Master confirms heartbeat");
                        break;
                    case "ERR":
                        Console.WriteLine("Master provided error response: {0}", resultData);
                        break;
                }
            }
        });
        
        // Function 2. send commands to master on primary thread
        bool endApp = false;
        while (!endApp)
        {
            Console.WriteLine("Enter command to issue");
            string? cmdToExecute = Console.ReadLine();

            Console.WriteLine("Connecting to master . .");
            using var requester = new RequestSocket();
            requester.Connect(String.Format("tcp://{0}", MASTER_ADDRESS));

            requester.SendMoreFrame(DS_CommandType.COMMAND.ToString())
                    .SendMoreFrame(WORKER_ADDRESS)
                    .SendFrame(cmdToExecute ?? String.Empty);

            NetMQMessage masterResponse = requester.ReceiveMultipartMessage();
            string resultType = masterResponse[0].ConvertToString();
            string resultData = masterResponse[1].ConvertToString();

            Console.WriteLine("Master responded {0} : {1} ...", resultType, resultData);
            
            // Prompt user whether to exit or hold on.
            Console.WriteLine("Press 'x' and Enter to close the app, or press any other key and Enter to continue: ");
            if (Console.ReadLine() == "x") endApp = true;
        }
    }
}
