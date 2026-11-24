using Mq.Broker;

string dataDir = args.Contains("--data") ? args[Array.IndexOf(args, "--data") + 1] : "./data";
int port = args.Contains("--port") ? int.Parse(args[Array.IndexOf(args, "--port") + 1]) : 9092;

using var server = new BrokerServer(dataDir, port);
server.Run();
Console.WriteLine($"mq-broker listening on 127.0.0.1:{server.Port}, data in {Path.GetFullPath(dataDir)}");
Console.WriteLine("Ctrl+C to stop.");

var stopped = new ManualResetEventSlim();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stopped.Set();
};
stopped.Wait();
