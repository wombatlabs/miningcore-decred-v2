using System.Buffers.Binary;
using System.Globalization;
using Autofac;
using Miningcore.Blockchain.Bitcoin.Configuration;
using Miningcore.Blockchain.Bitcoin.DaemonResponses;
using Miningcore.Blockchain.Bitcoin.Custom.Decred;
using Miningcore.Configuration;
using Miningcore.Contracts;
using Miningcore.Crypto;
using Miningcore.Extensions;
using Miningcore.JsonRpc;
using Miningcore.Messaging;
using Miningcore.Notifications.Messages;
using Miningcore.Rpc;
using Miningcore.Stratum;
using Miningcore.Time;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using Org.BouncyCastle.Crypto.Parameters;

namespace Miningcore.Blockchain.Bitcoin;

public class BitcoinJobManager : BitcoinJobManagerBase<BitcoinJob>
{
    public BitcoinJobManager(
        IComponentContext ctx,
        IMasterClock clock,
        IMessageBus messageBus,
        IExtraNonceProvider extraNonceProvider) :
        base(ctx, clock, messageBus, extraNonceProvider)
    {
    }

    private BitcoinTemplate coin;
    private bool isDecred;

    protected override object[] GetBlockTemplateParams()
    {
        var result = base.GetBlockTemplateParams();
        
        if(coin.HasMWEB)
        {
            result = new object[]
            {
                new
                {
                    rules = new[] {"segwit", "mweb"},
                }
            };
        }

        if(coin.BlockTemplateRpcExtraParams != null)
        {
            if(coin.BlockTemplateRpcExtraParams.Type == JTokenType.Array)
                result = result.Concat(coin.BlockTemplateRpcExtraParams.ToObject<object[]>() ?? Array.Empty<object>()).ToArray();
            else
                result = result.Concat(new []{ coin.BlockTemplateRpcExtraParams.ToObject<object>()}).ToArray();
        }

        return result;
    }
    
    protected override async Task EnsureDaemonsSynchedAsync(CancellationToken ct)
    {
        if(isDecred)
        {
            await EnsureDecredDaemonsSynchedAsync(ct);
            return;
        }

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));

        var syncPendingNotificationShown = false;

        do
        {
            var response = await rpc.ExecuteAsync<BlockTemplate>(logger,
                BitcoinCommands.GetBlockTemplate, ct, GetBlockTemplateParams());

            var isSynched = response.Error == null;

            if(isSynched)
            {
                logger.Info(() => "All daemons synched with blockchain");
                break;
            }
            else
            {
                logger.Debug(() => $"Daemon reports error: {response.Error?.Message}");
            }

            if(!syncPendingNotificationShown)
            {
                logger.Info(() => "Daemon is still syncing with network. Manager will be started once synced.");
                syncPendingNotificationShown = true;
            }

            await ShowDaemonSyncProgressAsync(ct);
        } while(await timer.WaitForNextTickAsync(ct));
    }

    private async Task EnsureDecredDaemonsSynchedAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        var syncPendingNotificationShown = false;

        do
        {
            var response = await rpc.ExecuteAsync<GetWorkResult>(logger,
                BitcoinCommands.GetWork, ct);

            var isSynched = response.Error == null;

            if(isSynched)
            {
                logger.Info(() => "All daemons synched with blockchain");
                break;
            }

            logger.Debug(() => $"Daemon reports error: {response.Error?.Message}");

            if(!syncPendingNotificationShown)
            {
                logger.Info(() => "Daemon is still syncing with network. Manager will be started once synced.");
                syncPendingNotificationShown = true;
            }

            await ShowDaemonSyncProgressAsync(ct);
        } while(await timer.WaitForNextTickAsync(ct));
    }

    protected async Task<RpcResponse<BlockTemplate>> GetBlockTemplateAsync(CancellationToken ct)
    {
        var result = await rpc.ExecuteAsync<BlockTemplate>(logger,
            BitcoinCommands.GetBlockTemplate, ct, extraPoolConfig?.GBTArgs ?? (object) GetBlockTemplateParams());

        return result;
    }

    protected RpcResponse<BlockTemplate> GetBlockTemplateFromJson(string json)
    {
        var result = JsonConvert.DeserializeObject<JsonRpcResponse>(json);

        return new RpcResponse<BlockTemplate>(result!.ResultAs<BlockTemplate>());
    }

    private BitcoinJob CreateJob()
    {
        switch(coin.Symbol)
        {
            case "DCR":
                return new DecredJob();
            default:
                return new();
        }
    }

    protected override void PostChainIdentifyConfigure()
    {
        base.PostChainIdentifyConfigure();

        if(poolConfig.EnableInternalStratum == true && coin.HeaderHasherValue is IHashAlgorithmInit hashInit)
        {
            if(!hashInit.DigestInit(poolConfig))
                logger.Error(()=> $"{hashInit.GetType().Name} initialization failed");
        }
    }

    protected override async Task<(bool IsNew, bool Force)> UpdateJob(CancellationToken ct, bool forceUpdate, string via = null, string json = null)
    {
        try
        {
            if(isDecred)
                return await UpdateJobDecred(ct, forceUpdate, via);

            if(forceUpdate)
                lastJobRebroadcast = clock.Now;

            var response = string.IsNullOrEmpty(json) ?
                await GetBlockTemplateAsync(ct) :
                GetBlockTemplateFromJson(json);

            // may happen if daemon is currently not connected to peers
            if(response.Error != null)
            {
                logger.Warn(() => $"Unable to update job. Daemon responded with: {response.Error.Message} Code {response.Error.Code}");
                return (false, forceUpdate);
            }

            var blockTemplate = response.Response;
            var job = currentJob;

            var isNew = job == null ||
                (blockTemplate != null &&
                    (job.BlockTemplate?.PreviousBlockhash != blockTemplate.PreviousBlockhash ||
                        blockTemplate.Height > job.BlockTemplate?.Height));

            if(isNew)
                messageBus.NotifyChainHeight(poolConfig.Id, blockTemplate.Height, poolConfig.Template);

            if(isNew || forceUpdate)
            {
                job = CreateJob();

                job.Init(blockTemplate, NextJobId(),
                    poolConfig, extraPoolConfig, clusterConfig, clock, poolAddressDestination, network, isPoS,
                    ShareMultiplier, coin.CoinbaseHasherValue, coin.HeaderHasherValue,
                    !isPoS ? coin.BlockHasherValue : coin.PoSBlockHasherValue ?? coin.BlockHasherValue);

                if(isNew)
                {
                    if(via != null)
                        logger.Info(() => $"Detected new block {blockTemplate.Height} [{via}]");
                    else
                        logger.Info(() => $"Detected new block {blockTemplate.Height}");

                    // update stats
                    BlockchainStats.LastNetworkBlockTime = clock.Now;
                    BlockchainStats.BlockHeight = blockTemplate.Height;
                    BlockchainStats.NetworkDifficulty = job.Difficulty;
                    BlockchainStats.NextNetworkTarget = blockTemplate.Target;
                    BlockchainStats.NextNetworkBits = blockTemplate.Bits;
                }

                else
                {
                    if(via != null)
                        logger.Debug(() => $"Template update {blockTemplate?.Height} [{via}]");
                    else
                        logger.Debug(() => $"Template update {blockTemplate?.Height}");
                }

                currentJob = job;
            }

            return (isNew, forceUpdate);
        }

        catch(OperationCanceledException)
        {
            // ignored
        }

        catch(Exception ex)
        {
            logger.Error(ex, () => $"Error during {nameof(UpdateJob)}");
        }

        return (false, forceUpdate);
    }

    private async Task<(bool IsNew, bool Force)> UpdateJobDecred(CancellationToken ct, bool forceUpdate, string via)
    {
        try
        {
            if(forceUpdate)
                lastJobRebroadcast = clock.Now;

            var response = await rpc.ExecuteAsync<GetWorkResult>(logger, BitcoinCommands.GetWork, ct);

            if(response.Error != null)
            {
                logger.Warn(() => $"Unable to update job. Daemon responded with: {response.Error.Message} Code {response.Error.Code}");
                return (false, forceUpdate);
            }

            var work = response.Response;
            if(work == null || string.IsNullOrEmpty(work.Data))
                return (false, forceUpdate);

            var blockTemplate = await CreateDecredBlockTemplateAsync(work, ct);
            var job = currentJob;

            var isNew = job == null ||
                job.BlockTemplate?.Hex != blockTemplate.Hex;

            if(isNew)
                messageBus.NotifyChainHeight(poolConfig.Id, blockTemplate.Height, poolConfig.Template);

            if(isNew || forceUpdate)
            {
                job = CreateJob();

                await job.InitLegacy(blockTemplate, NextJobId(),
                    poolConfig, extraPoolConfig, clusterConfig, clock, poolAddressDestination, network, isPoS,
                    ShareMultiplier, coin.CoinbaseHasherValue, coin.HeaderHasherValue,
                    !isPoS ? coin.BlockHasherValue : coin.PoSBlockHasherValue ?? coin.BlockHasherValue, rpc);

                if(isNew)
                {
                    if(via != null)
                        logger.Info(() => $"Detected new block {blockTemplate.Height} [{via}]");
                    else
                        logger.Info(() => $"Detected new block {blockTemplate.Height}");

                    BlockchainStats.LastNetworkBlockTime = clock.Now;
                    BlockchainStats.BlockHeight = blockTemplate.Height;
                    BlockchainStats.NetworkDifficulty = job.Difficulty;
                    BlockchainStats.NextNetworkTarget = blockTemplate.Target;
                    BlockchainStats.NextNetworkBits = blockTemplate.Bits;
                }

                else
                {
                    if(via != null)
                        logger.Debug(() => $"Template update {blockTemplate?.Height} [{via}]");
                    else
                        logger.Debug(() => $"Template update {blockTemplate?.Height}");
                }

                currentJob = job;
            }

            return (isNew, forceUpdate);
        }

        catch(OperationCanceledException)
        {
            // ignored
        }

        catch(Exception ex)
        {
            logger.Error(ex, () => $"Error during {nameof(UpdateJobDecred)}");
        }

        return (false, forceUpdate);
    }

    protected override object GetJobParamsForStratum(bool isNew)
    {
        var job = currentJob;
        return job?.GetJobParams(isNew);
    }

    public override BitcoinJob GetJobForStratum()
    {
        var job = currentJob;
        return job;
    }

    private async Task<BlockTemplate> CreateDecredBlockTemplateAsync(GetWorkResult work, CancellationToken ct)
    {
        if(string.IsNullOrEmpty(work.Data) || work.Data.Length < DecredJob.HeaderHexLength)
            throw new StratumException(StratumError.Other, "invalid decred work data");

        var headerHex = work.Data.Substring(0, DecredJob.HeaderHexLength);
        var prevBlockLe = headerHex.Substring(8, 64);
        var prevBlockHash = prevBlockLe.HexToByteArray().ReverseByteOrder().ToHexString();
        var version = ReadUInt32LE(headerHex, 0);
        var bits = ReadUInt32LE(headerHex, 232);
        var height = ReadUInt32LE(headerHex, 256);
        var nTime = ReadUInt32LE(headerHex, 272);
        var voters = ReadUInt16LE(headerHex, 216);

        var coinbaseValue = await FetchDecredPowRewardAsync(height, voters, ct);

        return new BlockTemplate
        {
            Hex = work.Data,
            Version = version,
            PreviousBlockhash = prevBlockHash,
            Bits = bits.ToString("x8", CultureInfo.InvariantCulture),
            CurTime = nTime,
            Height = height,
            Target = work.Target,
            CoinbaseValue = coinbaseValue,
            Transactions = Array.Empty<BitcoinBlockTransaction>()
        };
    }

    private async Task<long> FetchDecredPowRewardAsync(uint height, ushort voters, CancellationToken ct)
    {
        try
        {
            var response = await rpc.ExecuteAsync<DecredBlockSubsidy>(logger,
                BitcoinCommands.GetBlockSubsidy, ct, new object[] { (int) height, (int) voters });

            if(response.Error != null)
            {
                logger.Warn(() => $"Unable to fetch block subsidy. Daemon responded with: {response.Error.Message} Code {response.Error.Code}");
                return 0;
            }

            return response.Response?.PoW ?? 0;
        }

        catch(Exception ex)
        {
            logger.Warn(ex, () => $"Unable to fetch block subsidy for height {height}");
            return 0;
        }
    }

    private static uint ReadUInt32LE(string hex, int start)
    {
        var bytes = hex.Substring(start, 8).HexToByteArray();
        return BinaryPrimitives.ReadUInt32LittleEndian(bytes);
    }

    private static ushort ReadUInt16LE(string hex, int start)
    {
        var bytes = hex.Substring(start, 4).HexToByteArray();
        return BinaryPrimitives.ReadUInt16LittleEndian(bytes);
    }

    #region API-Surface

    public override void Configure(PoolConfig pc, ClusterConfig cc)
    {
        coin = pc.Template.As<BitcoinTemplate>();
        isDecred = string.Equals(coin.Symbol, "DCR", StringComparison.OrdinalIgnoreCase);
        extraPoolConfig = pc.Extra.SafeExtensionDataAs<BitcoinPoolConfigExtra>();
        extraPoolPaymentProcessingConfig = pc.PaymentProcessing?.Extra?.SafeExtensionDataAs<BitcoinPoolPaymentProcessingConfigExtra>();

        if(extraPoolConfig?.MaxActiveJobs.HasValue == true)
            maxActiveJobs = extraPoolConfig.MaxActiveJobs.Value;

        hasLegacyDaemon = extraPoolConfig?.HasLegacyDaemon == true;

        base.Configure(pc, cc);
    }

    public virtual object[] GetSubscriberData(StratumConnection worker)
    {
        Contract.RequiresNonNull(worker);

        var context = worker.ContextAs<BitcoinWorkerContext>();

        // assign unique ExtraNonce1 to worker (miner)
        context.ExtraNonce1 = extraNonceProvider.Next();

        // setup response data
        var responseData = new object[]
        {
            context.ExtraNonce1,
            BitcoinConstants.ExtranoncePlaceHolderLength - ExtranonceBytes,
        };

        return responseData;
    }

    public virtual async ValueTask<Share> SubmitShareAsync(StratumConnection worker, object submission,
        CancellationToken ct)
    {
        Contract.RequiresNonNull(worker);
        Contract.RequiresNonNull(submission);

        if(submission is not object[] submitParams)
            throw new StratumException(StratumError.Other, "invalid params");

        var context = worker.ContextAs<BitcoinWorkerContext>();

        // extract params
        var workerValue = (submitParams[0] as string)?.Trim();
        var jobId = submitParams[1] as string;
        var extraNonce2 = submitParams[2] as string;
        var nTime = submitParams[3] as string;
        var nonce = submitParams[4] as string;
        var versionBits = context.VersionRollingMask.HasValue ? submitParams[5] as string : null;

        if(string.IsNullOrEmpty(workerValue))
            throw new StratumException(StratumError.Other, "missing or invalid workername");

        BitcoinJob job;

        lock(context)
        {
            job = context.GetJob(jobId);
        }

        if(job == null)
            throw new StratumException(StratumError.JobNotFound, "job not found");

        // validate & process
        var (share, blockHex) = job.ProcessShare(worker, extraNonce2, nTime, nonce, versionBits);

        // enrich share with common data
        share.PoolId = poolConfig.Id;
        share.IpAddress = worker.RemoteEndpoint.Address.ToString();
        share.Miner = context.Miner;
        share.Worker = context.Worker;
        share.UserAgent = context.UserAgent;
        share.Source = clusterConfig.ClusterName;
        share.Created = clock.Now;

        // if block candidate, submit & check if accepted by network
        if(share.IsBlockCandidate)
        {
            logger.Info(() => $"Submitting block {share.BlockHeight} [{share.BlockHash}]");

            var acceptResponse = isDecred
                ? await SubmitDecredBlockAsync(share, blockHex, ct)
                : await SubmitBlockAsync(share, blockHex, ct);

            // is it still a block candidate?
            share.IsBlockCandidate = acceptResponse.Accepted;

            if(share.IsBlockCandidate)
            {
                logger.Info(() => $"Daemon accepted block {share.BlockHeight} [{share.BlockHash}] submitted by {context.Miner}");

                OnBlockFound();

                // persist the coinbase transaction-hash to allow the payment processor
                // to verify later on that the pool has received the reward for the block
                share.TransactionConfirmationData = acceptResponse.CoinbaseTx;
            }

            else
            {
                // clear fields that no longer apply
                share.TransactionConfirmationData = null;
            }
        }

        return share;
    }

    private async Task<SubmitResult> SubmitDecredBlockAsync(Share share, string submission, CancellationToken ct)
    {
        if(string.IsNullOrEmpty(submission))
            return new SubmitResult(false, null);

        var submitResponse = await rpc.ExecuteAsync<JToken>(logger, BitcoinCommands.GetWork, ct, new[] { submission });
        var submitAccepted = submitResponse.Response?.Value<bool>() ?? false;

        if(submitResponse.Error != null || submitAccepted == false)
        {
            var submitError = submitResponse.Error?.Message ??
                submitResponse.Error?.Code.ToString(CultureInfo.InvariantCulture) ??
                "rejected";

            logger.Warn(() => $"Block {share.BlockHeight} submission failed with: {submitError}");
            messageBus.SendMessage(new AdminNotification("Block submission failed", $"Pool {poolConfig.Id} {(!string.IsNullOrEmpty(share.Source) ? $"[{share.Source.ToUpper()}] " : string.Empty)}failed to submit block {share.BlockHeight}: {submitError}"));
            return new SubmitResult(false, null);
        }

        var blockResponse = await rpc.ExecuteAsync<DaemonResponses.Block>(logger, BitcoinCommands.GetBlock, ct, new[] { share.BlockHash });
        var block = blockResponse.Response;
        var accepted = blockResponse.Error == null && block?.Hash == share.BlockHash;

        if(!accepted)
        {
            logger.Warn(() => $"Block {share.BlockHeight} submission failed for pool {poolConfig.Id} because block was not found after submission");
            messageBus.SendMessage(new AdminNotification($"[{poolConfig.Id}]-[{(!string.IsNullOrEmpty(share.Source) ? $"[{share.Source.ToUpper()}] " : string.Empty)}] Block submission failed", $"[{poolConfig.Id}]-[{(!string.IsNullOrEmpty(share.Source) ? $"[{share.Source.ToUpper()}] " : string.Empty)}] Block {share.BlockHeight} submission failed for pool {poolConfig.Id} because block was not found after submission"));
        }

        return new SubmitResult(accepted, block?.Transactions?.FirstOrDefault());
    }

    public double ShareMultiplier => coin.ShareMultiplier;

    #endregion // API-Surface
}
