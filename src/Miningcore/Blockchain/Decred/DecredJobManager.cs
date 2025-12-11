using System.Buffers.Binary;
using System.Globalization;
using Autofac;
using Miningcore.Blockchain.Bitcoin;
using Miningcore.Blockchain.Bitcoin.Configuration;
using Miningcore.Blockchain.Bitcoin.DaemonResponses;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.Messaging;
using Miningcore.Notifications.Messages;
using Miningcore.Rpc;
using Miningcore.Stratum;
using Miningcore.Time;
using Newtonsoft.Json.Linq;
using NLog;

using BitcoinDaemonResponses = Miningcore.Blockchain.Bitcoin.DaemonResponses;

namespace Miningcore.Blockchain.Decred;

public class DecredJobManager : BitcoinJobManager
{
    public DecredJobManager(
        IComponentContext ctx,
        IMasterClock clock,
        IMessageBus messageBus,
        IExtraNonceProvider extraNonceProvider) :
        base(ctx, clock, messageBus, extraNonceProvider)
    {
    }

    protected override BitcoinJob CreateJob() => new DecredJob();

    protected override async Task EnsureDaemonsSynchedAsync(CancellationToken ct)
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

    protected override async Task<(bool IsNew, bool Force)> UpdateJob(CancellationToken ct, bool forceUpdate, string via = null, string json = null)
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
            logger.Error(ex, () => $"Error during {nameof(UpdateJob)}");
        }

        return (false, forceUpdate);
    }

    protected override async Task<SubmitResult> SubmitBlockAsync(Share share, string submission, CancellationToken ct)
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

        var blockResponse = await rpc.ExecuteAsync<BitcoinDaemonResponses.Block>(logger, BitcoinCommands.GetBlock, ct, new[] { share.BlockHash });
        var block = blockResponse.Response;
        var accepted = blockResponse.Error == null && block?.Hash == share.BlockHash;

        if(!accepted)
        {
            logger.Warn(() => $"Block {share.BlockHeight} submission failed for pool {poolConfig.Id} because block was not found after submission");
            messageBus.SendMessage(new AdminNotification($"[{poolConfig.Id}]-[{(!string.IsNullOrEmpty(share.Source) ? $"[{share.Source.ToUpper()}] " : string.Empty)}] Block submission failed", $"[{poolConfig.Id}]-[{(!string.IsNullOrEmpty(share.Source) ? $"[{share.Source.ToUpper()}] " : string.Empty)}] Block {share.BlockHeight} submission failed for pool {poolConfig.Id} because block was not found after submission"));
        }

        return new SubmitResult(accepted, block?.Transactions?.FirstOrDefault());
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

        var targetHex = work.Target;

        if(!string.IsNullOrEmpty(targetHex))
            targetHex = targetHex.HexToByteArray().ReverseByteOrder().ToHexString();

        return new BlockTemplate
        {
            Hex = work.Data,
            Version = version,
            PreviousBlockhash = prevBlockHash,
            Bits = bits.ToString("x8", CultureInfo.InvariantCulture),
            CurTime = nTime,
            Height = height,
            Target = targetHex,
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
}
