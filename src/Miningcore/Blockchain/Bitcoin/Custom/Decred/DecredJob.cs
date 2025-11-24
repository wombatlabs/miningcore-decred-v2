using System.Buffers.Binary;
using System.Threading.Tasks;
using Miningcore.Blockchain.Bitcoin.Configuration;
using Miningcore.Blockchain.Bitcoin.DaemonResponses;
using Miningcore.Configuration;
using Miningcore.Contracts;
using Miningcore.Crypto;
using Miningcore.Extensions;
using Miningcore.Stratum;
using Miningcore.Time;
using Miningcore.Util;
using Miningcore.Rpc;
using NBitcoin;

namespace Miningcore.Blockchain.Bitcoin.Custom.Decred
{
    public class DecredJob : BitcoinJob
    {
        internal const int HeaderHexLength = 360;

        private string workData;
        private string prevBlockLe;
        private string genTx1;
        private string blockVersion;
        private string nBits;
        private string baseNTime;

        public override async Task InitLegacy(BlockTemplate bt, string jobId,
            PoolConfig pc, BitcoinPoolConfigExtra extraPoolConfig,
            ClusterConfig cc, IMasterClock clock,
            IDestination poolAddressDestination, Network network,
            bool isPoS, double shareMultiplier, IHashAlgorithm coinbaseHasher,
            IHashAlgorithm headerHasher, IHashAlgorithm blockHasher, RpcClient rpc)
        {
            Contract.RequiresNonNull(bt);
            Contract.Requires<ArgumentException>(!string.IsNullOrEmpty(bt.Hex));

            workData = bt.Hex;

            if(workData.Length < HeaderHexLength)
                throw new StratumException(StratumError.Other, "invalid decred work template");

            var headerHex = workData.Substring(0, HeaderHexLength);

            blockVersion = headerHex[..8];
            prevBlockLe = headerHex.Substring(8, 64);
            genTx1 = headerHex.Substring(72, HeaderHexLength - 72);
            nBits = headerHex.Substring(232, 8);
            baseNTime = headerHex.Substring(272, 8);

            if(bt.Transactions == null)
                bt.Transactions = Array.Empty<BitcoinBlockTransaction>();

            await base.InitLegacy(bt, jobId, pc, extraPoolConfig, cc, clock, poolAddressDestination, network,
                isPoS, shareMultiplier, coinbaseHasher, headerHasher, blockHasher, rpc);

            jobParams = new object[]
            {
                JobId,
                prevBlockLe,
                genTx1,
                string.Empty,
                Array.Empty<string>(),
                blockVersion,
                nBits,
                baseNTime,
                false
            };

            return;
        }

        public override (Share Share, string BlockHex) ProcessShare(StratumConnection worker,
            string extraNonce2, string nTime, string nonce, string versionBits = null)
        {
            Contract.RequiresNonNull(worker);
            var context = worker.ContextAs<BitcoinWorkerContext>();

            if(string.IsNullOrEmpty(extraNonce2) || extraNonce2.Length != 8)
                throw new StratumException(StratumError.Other, "incorrect size of extranonce2");

            if(nTime.Length != 8)
                throw new StratumException(StratumError.Other, "incorrect size of ntime");

            if(nonce.Length != 8)
                throw new StratumException(StratumError.Other, "incorrect size of nonce");

            var nTimeInt = ParseUInt32LE(nTime);

            if(nTimeInt < BlockTemplate.CurTime || nTimeInt > ((DateTimeOffset) clock.Now).ToUnixTimeSeconds() + 7200)
                throw new StratumException(StratumError.Other, "ntime out of range");

            if(!RegisterSubmit(context.ExtraNonce1, extraNonce2, nTime, nonce))
                throw new StratumException(StratumError.DuplicateShare, "duplicate share");

            var submission = BuildSubmission(context.ExtraNonce1, extraNonce2, nTime, nonce);
            var headerBytes = submission.Substring(0, HeaderHexLength).HexToByteArray();

            Span<byte> headerHash = stackalloc byte[32];
            headerHasher.Digest(headerBytes, headerHash, (ulong) nTimeInt, BlockTemplate, coin, networkParams);
            var headerValue = new uint256(headerHash);

            var shareDiff = (double) new BigRational(BitcoinConstants.Diff1, headerHash.ToBigInteger()) * shareMultiplier;
            var stratumDifficulty = context.Difficulty;
            var ratio = shareDiff / stratumDifficulty;

            if(ratio < 0.99)
            {
                if(context.VarDiff?.LastUpdate != null && context.PreviousDifficulty.HasValue)
                {
                    ratio = shareDiff / context.PreviousDifficulty.Value;

                    if(ratio < 0.99)
                        throw new StratumException(StratumError.LowDifficultyShare, $"low difficulty share ({shareDiff})");

                    stratumDifficulty = context.PreviousDifficulty.Value;
                }

                else
                    throw new StratumException(StratumError.LowDifficultyShare, $"low difficulty share ({shareDiff})");
            }

            var isBlockCandidate = headerValue <= blockTargetValue;

            var result = new Share
            {
                BlockHeight = BlockTemplate.Height,
                NetworkDifficulty = Difficulty,
                Difficulty = stratumDifficulty / shareMultiplier,
            };

            if(isBlockCandidate)
            {
                result.IsBlockCandidate = true;

                Span<byte> blockHash = stackalloc byte[32];
                blockHasher.Digest(headerBytes, blockHash, nTimeInt);
                result.BlockHash = blockHash.ToHexString();

                return (result, submission);
            }

            return (result, null);
        }

        private string BuildSubmission(string extraNonce1, string extraNonce2, string nTime, string nonce)
        {
            var buffer = workData.ToCharArray();

            Replace(buffer, 272, nTime);
            Replace(buffer, 280, nonce);
            Replace(buffer, 288, extraNonce1);
            Replace(buffer, 296, extraNonce2);

            return new string(buffer);
        }

        private static void Replace(char[] buffer, int startIndex, string value)
        {
            for(var i = 0; i < value.Length; i++)
                buffer[startIndex + i] = value[i];
        }

        private static uint ParseUInt32LE(string hex)
        {
            var bytes = hex.HexToByteArray();
            return BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        }
    }
}
