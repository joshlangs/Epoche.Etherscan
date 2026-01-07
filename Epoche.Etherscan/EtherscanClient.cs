using System.Net;
using Microsoft.Extensions.Options;

namespace Epoche.Etherscan;

public class EtherscanClient
{
    const string MagicRateLimitFragment = "rate limit reached";

    readonly object LockObject = new();
    readonly HttpClient Client;
    readonly string? ApiKey;
    readonly TimeSpan MinCallInterval;
    readonly string QueryStart;

    static readonly (string Key, string Value) TagLatest = ("tag", "latest");
    static readonly JsonSerializerOptions Options = new()
    {
        Converters =
        {
            Int64Converter.Instance,
            BigFractionConverter.Instance
        },
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString
    };

    DateTime NextApiCall = DateTime.UtcNow;

    public EtherscanClient(string endpoint = KnownEndpoints.Ethereum, string? apiKey = null, TimeSpan? minCallInterval = null)
    {
        Client = new HttpClient() { BaseAddress = new Uri(endpoint) };
        ApiKey = apiKey;
        MinCallInterval = minCallInterval ?? (string.IsNullOrEmpty(apiKey) ? EtherscanClientOptions.DefaultMinCallIntervalWithoutKey : EtherscanClientOptions.DefaultMinCallIntervalWithKey);
        QueryStart = endpoint.Contains('?') ? "&" : "?";
    }
    public EtherscanClient(IOptions<EtherscanClientOptions> options) : this(options.Value.Endpoint, options.Value.ApiKey, options.Value.MinCallInterval)
    {
    }

    public EtherscanERC20Client ERC20(string contractAddress) => new(Call, contractAddress);
    public EtherscanLiquidityPairClient LiquidityPair(string contractAddress) => new(Call, contractAddress);
    EtherscanCallClient? call;
    public EtherscanCallClient Call => call ??= new(this);

    async Task RateLimitAsync(CancellationToken cancellationToken)
    {
        TimeSpan next;
        while (true)
        {
            lock (LockObject)
            {
                next = NextApiCall.Subtract(DateTime.UtcNow);
                if (next <= TimeSpan.Zero)
                {
                    NextApiCall = DateTime.UtcNow.Add(MinCallInterval);
                    return;
                }
            }
            await Task.Delay(next, cancellationToken).ConfigureAwait(false);
        }
    }

    internal async Task<T> GetResultAsync<T>(string module, string action, CancellationToken cancellationToken, params (string Key, string Value)[] parameters) where T : class
    {
        var callParams = new List<(string Key, string Value)>(3)
        {
            ("module", module),
            ("action", action)
        };
        if (!string.IsNullOrEmpty(ApiKey))
        {
            callParams.Add(("apikey", ApiKey));
        }

        var url = QueryStart + string
            .Join("&", parameters
                .Concat(callParams)
                .Select(x => $"{x.Key}={x.Value}"));
        while (true)
        {
            await RateLimitAsync(cancellationToken).ConfigureAwait(false);
            using var getResult = await Client.GetAsync(url, cancellationToken).ConfigureAwait(false);
            if (getResult.StatusCode == HttpStatusCode.TooManyRequests)
            {
                continue;
            }
            var resultString = await getResult.EnsureSuccessStatusCode().Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            var doc = JsonDocument.Parse(resultString);
            if (doc.RootElement.TryGetProperty("status", out var statusProp) &&
                statusProp.ValueKind == JsonValueKind.String &&
                statusProp.GetString() == "0" &&
                doc.RootElement.TryGetProperty("result", out var resultProp) &&
                resultProp.ValueKind == JsonValueKind.String)
            {
                var errorResult = JsonSerializer.Deserialize<EtherscanResult<string>>(resultString, Options)!;
                if (errorResult.Result?.Contains(MagicRateLimitFragment) == true)
                {
                    continue;
                }
                throw new EtherscanException(errorResult);
            }
            var result = JsonSerializer.Deserialize<EtherscanResult<T>>(resultString, Options);
            if (result is null)
            {
                throw new EtherscanException("Null result returned from the API");
            }
            return result.Result!;
        }
    }

    async Task<T[]> GetMultiPageResultAsync<T>(string module, string action, CancellationToken cancellationToken, params (string Key, string Value)[] parameters) where T : class
    {
        const int PageSize = 10000;
        List<T> results = new();
        for (var page = 1; ; ++page)
        {
            var p = parameters.ToList();
            p.Add(("page", page.ToString()));
            p.Add(("offset", PageSize.ToString()));
            p.Add(("sort", "asc"));
            var pagedResults = await GetResultAsync<T[]>(module, action, cancellationToken, p.ToArray()).ConfigureAwait(false);
            results.AddRange(pagedResults);
            if (pagedResults.Length < PageSize)
            {
                return results.ToArray();
            }
        }
    }

    public Task<Dictionary<string, BigFraction>> GetNativeBalancesAsync(params string[] addresses) => GetNativeBalancesAsync(CancellationToken.None, addresses);
    public async Task<Dictionary<string, BigFraction>> GetNativeBalancesAsync(CancellationToken cancellationToken, params string[] addresses)
    {
        const int MaxPerCall = 20;
        List<NativeBalanceResult> results = new(addresses.Length);
        foreach (var addrs in addresses.Segment(MaxPerCall))
        {
            var result = await GetResultAsync<NativeBalanceResult[]>("account", "balancemulti", cancellationToken, TagLatest, ("address", string.Join(",", addrs))).ConfigureAwait(false);
            results.AddRange(result);
        }
        return results.ToDictionary(x => x.Account, x => x.Balance);
    }

    public Task<TransactionResult[]> GetNormalTransactionsAsync(string address, long startBlock = 0, long endBlock = 9999999999999999L, CancellationToken cancellationToken = default) =>
        GetMultiPageResultAsync<TransactionResult>("account", "txlist", cancellationToken, ("address", address), ("startblock", startBlock.ToString()), ("endblock", endBlock.ToString()));

    public async Task<BigFraction> GetTokenBalanceAsync(string address, string contractAddress, CancellationToken cancellationToken = default)
    {
        var result = await GetResultAsync<string>("account", "tokenbalance", cancellationToken, TagLatest, ("address", address), ("contractaddress", contractAddress)).ConfigureAwait(false);
        return result.FromWei();
    }

    public Task<TokenTransferResult[]> GetTokenTransfersAsync(string? address, string? contractAddress = null, long startBlock = 0, long endBlock = 9999999999999999L, CancellationToken cancellationToken = default)
    {
        var p = new List<(string, string)>
        {
            ("startblock", startBlock.ToString()),
            ("endblock", endBlock.ToString())
        };
        if (!string.IsNullOrEmpty(address))
        {
            p.Add(("address", address));
        }
        if (!string.IsNullOrEmpty(contractAddress))
        {
            p.Add(("contractaddress", contractAddress));
        }
        return GetMultiPageResultAsync<TokenTransferResult>("account", "tokentx", cancellationToken, p.ToArray());
    }

    public Task<ERC721TokenTransferResult[]> GetERC721TokenTransfersAsync(string? address, string? contractAddress = null, long startBlock = 0, long endBlock = 9999999999999999L, CancellationToken cancellationToken = default)
    {
        var p = new List<(string, string)>
        {
            ("startblock", startBlock.ToString()),
            ("endblock", endBlock.ToString())
        };
        if (!string.IsNullOrEmpty(address))
        {
            p.Add(("address", address));
        }
        if (!string.IsNullOrEmpty(contractAddress))
        {
            p.Add(("contractaddress", contractAddress));
        }
        return GetMultiPageResultAsync<ERC721TokenTransferResult>("account", "tokennfttx", cancellationToken, p.ToArray());
    }

    public Task<InternalTransactionResult[]> GetInternalTransactionsAsync(string? address = null, string? transactionHash = null, long startBlock = 0, long endBlock = 9999999999999999L, CancellationToken cancellationToken = default)
    {
        var p = new List<(string, string)>
        {
            ("startblock", startBlock.ToString()),
            ("endblock", endBlock.ToString())
        };
        if (!string.IsNullOrEmpty(address))
        {
            p.Add(("address", address));
        }
        if (!string.IsNullOrEmpty(transactionHash))
        {
            p.Add(("txhash", transactionHash));
        }
        return GetMultiPageResultAsync<InternalTransactionResult>("account", "txlistinternal", cancellationToken, p.ToArray());
    }

    public async Task<TransactionResult?> GetNormalTransactionAsync(string transactionHash, CancellationToken cancellationToken = default)
    {
        var proxyTx = await GetResultAsync<ProxyTransactionResult>("proxy", "eth_getTransactionByHash", cancellationToken, ("txhash", transactionHash)).ConfigureAwait(false);
        if (proxyTx is null)
        {
            return null;
        }
        var blockNumber = proxyTx.BlockNumber.HexToLong();
        var txes = await GetNormalTransactionsAsync(address: proxyTx.From, startBlock: blockNumber, endBlock: blockNumber, cancellationToken: cancellationToken).ConfigureAwait(false);
        return txes.Single(x => x.Hash.Equals(transactionHash, StringComparison.OrdinalIgnoreCase));
    }

    public async Task<long> GetBlockNumberAsync(CancellationToken cancellationToken = default)
    {
        var blockNumber = await GetResultAsync<string>("proxy", "eth_blockNumber", cancellationToken).ConfigureAwait(false);
        return blockNumber.HexToLong();
    }

    public Task<string> GetCodeAsync(string address, CancellationToken cancellationToken = default) =>
        GetResultAsync<string>("proxy", "eth_getCode", cancellationToken, ("address", address));

    public async Task<BigFraction> GetGasPriceAsync(CancellationToken cancellationToken = default)
    {
        var gasPrice = await GetResultAsync<string>("proxy", "eth_gasPrice", cancellationToken).ConfigureAwait(false);
        return gasPrice.HexToWei();
    }

    public Task<string> SendRawTransactionAsync(string hex, CancellationToken cancellationToken = default) =>
        GetResultAsync<string>("proxy", "eth_sendRawTransaction", cancellationToken, ("hex", hex));

    public async Task<long> GetTransactionCountAsync(string address, CancellationToken cancellationToken = default)
    {
        var count = await GetResultAsync<string>("proxy", "eth_getTransactionCount", cancellationToken, ("address", address), TagLatest).ConfigureAwait(false);
        return count.HexToLong();
    }

}
