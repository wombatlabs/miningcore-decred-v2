namespace Miningcore.Blockchain.Bitcoin.DaemonResponses;

public class GetWorkResult
{
    public string Data { get; set; }
    public string Target { get; set; }
}

public class DecredBlockSubsidy
{
    public long Developer { get; set; }
    public long PoS { get; set; }
    public long PoW { get; set; }
    public long Total { get; set; }
}