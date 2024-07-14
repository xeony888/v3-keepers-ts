const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { createClient } = require('redis');
const bs58 = require('bs58');
const { Keypair, Connection, PublicKey } = require('@solana/web3.js');
const { ParclV3Sdk, getExchangePda, getMarketPda, ExchangeWrapper, translateAddress, MarginAccountWrapper, MarketWrapper } = require('@parcl-oss/v3-sdk');
const dotenv = require("dotenv");
dotenv.config();
(async function main() {
    const redis = createClient();
    await redis.connect();
    const [ exchangeAddress ] = getExchangePda(0);
    const commitment = process.env.COMMITMENT;
    const liquidatorMarginAccount = translateAddress(process.env.LIQUIDATOR_MARGIN_ACCOUNT);
    const liquidatorSigner = Keypair.fromSecretKey(bs58.decode(process.env.PRIVATE_KEY));
    const interval = parseInt(process.env.INTERVAL ?? "300");
    const sdk = new ParclV3Sdk({ rpcUrl: process.env.RPC_URL, commitment });
    const connection = new Connection(process.env.RPC_URL);
    const exchange = await sdk.accountFetcher.getExchange(exchangeAddress);
    if (!exchange) throw new Error("Could not get exchange");
    const exchangeWrapper = new ExchangeWrapper(exchange);
    const allMarketAddresses = exchange.marketIds.filter(marketId => marketId != 0).map(marketId => getMarketPda(exchangeAddress, marketId)[ 0 ]);
    const { task } = workerData;
    if (task === "activeMarginAccounts") {
        console.log("Active margin accounts worker started");
        const updateActiveMarginAccounts = async () => {
            const activeMarginAccounts = await retrieveAllActiveMarginAccounts(sdk, exchange, exchangeWrapper, allMarketAddresses);
            redis.set("activeMarginAccounts", JSON.stringify(activeMarginAccounts));
        };
        setInterval(updateActiveMarginAccounts, 60 * 1000);
    } else if (task === "accountCheckAndLiquidate") {
        const { slice, interval } = workerData;
        console.log(`Worker started, checking ${slice[ 0 ]}-${slice[ 1 ]} every ${interval}s`);
        setInterval(async () => {
            let data = JSON.parse(await redis.get("activeMarginAccounts")); /// TODO: optimize redis storage to store as array not as string
            if (data) {
                const sliced = data.slice(slice[ 0 ], slice[ 1 ]);
                const pubkeys = sliced.map(d => d.address);
                let retrieved = await sdk.accountFetcher.getMarginAccounts(pubkeys);
                /// TODO: store this in db
                const allMarkets = await sdk.accountFetcher.getMarkets(allMarketAddresses);
                const [ markets, priceFeeds ] = await getMarketMapAndPriceFeedMap(sdk, allMarkets);
                retrieved = retrieved.map(account => {
                    const marginAccount = new MarginAccountWrapper(
                        account.account,
                        account.address,
                    );
                    const margins = marginAccount.getAccountMargins(exchangeWrapper, markets, priceFeeds, now_seconds());
                    if (margins.canLiquidate()) {
                        liquidate(
                            sdk,
                            connection,
                            marginAccount,
                            {
                                marginAccount: account.address,
                                exchange: account.account.exchange,
                                owner: account.account.owner,
                                liquidator: liquidatorSigner.publicKey,
                                liquidatorMarginAccount,
                            },
                            markets,
                            [ liquidatorSigner ],
                            liquidatorSigner.publicKey
                        );
                        /// liquidate
                        return undefined;
                    } else {
                        return { margins, distance: distanceToLiquidation(margins), address: account.address };
                    }
                }).filter(a => a !== undefined);
                const before = data.slice(0, slice[ 0 ]);
                const after = data.slice(slice[ 1 ], data.length);
                retrieved = retrieved.map(margin => {
                    return { ...margin, distance: distanceToLiquidation(margin.margins) };
                }).sort((a, b) => a.distance.sub(b.distance).val.toNumber());
                const newArray = [ ...before, ...after ];
                for (const item of retrieved) {
                    const position = newArray.findIndex(margin => margin.distance > item.distance);
                    if (position === -1) {
                        newArray.push(item);
                    } else {
                        newArray.splice(position, 0, item);
                    }
                }
                await redis.set("activeMarginAccounts", JSON.stringify(newArray));
            }
        }, interval * 1000);
    }
})();


async function retrieveAllActiveMarginAccounts(sdk, exchange, exchangeWrapper, allMarketAddresses) {
    const allMarkets = await sdk.accountFetcher.getMarkets(allMarketAddresses);
    const [ markets, priceFeeds ] = await getMarketMapAndPriceFeedMap(sdk, allMarkets);
    const allMarginAccounts = await sdk.accountFetcher.getAllMarginAccounts();
    console.log(`retrieved ${allMarginAccounts.length} margin accounts for exchange ${exchange.id.toString()}`);
    let margins = allMarginAccounts.map(raw => {
        const marginAccount = new MarginAccountWrapper(
            raw.account,
            raw.address
        );
        const margins = marginAccount.getAccountMargins(exchangeWrapper, markets, priceFeeds, now_seconds());
        return { margins, address: raw.address };
    });
    console.log(`Total margin accounts: ${margins.length}`);
    margins = margins.filter((m) => isUsed(m.margins)).map(margin => {
        return { ...margin, distance: distanceToLiquidation(margin.margins) };
    }).sort((a, b) => a.distance.sub(b.distance).val.toNumber());
    console.log(margins[ 0 ]);
    console.log(`Active margin accounts: ${margins.length}`);
    return margins;
}
function now_seconds() {
    return Math.floor(Date.now() / 1000);
}
async function getMarketMapAndPriceFeedMap(
    sdk,
    allMarkets
) {
    const markets = {};
    for (const market of allMarkets) {
        if (market === undefined) {
            continue;
        }
        markets[ market.account.id ] = new MarketWrapper(market.account, market.address);
    }
    const allPriceFeedAddresses = (allMarkets).map(
        (market) => market.account.priceFeed
    );
    const allPriceFeeds = await sdk.accountFetcher.getPythPriceFeeds(allPriceFeedAddresses);
    const priceFeeds = {};
    for (let i = 0; i < allPriceFeeds.length; i++) {
        const priceFeed = allPriceFeeds[ i ];
        if (priceFeed === undefined) {
            continue;
        }
        priceFeeds[ allPriceFeedAddresses[ i ] ] = priceFeed;
    }
    return [ markets, priceFeeds ];
}

function distanceToLiquidation(m) {
    return m.margins.availableMargin.sub(m.totalRequiredMargin());
}

function isUsed(m) {
    for (const key in m.margins) {
        // @ts-ignore
        if (m.margins[ key ].val.eq(0)) {
            return false;
        }
    }
    return true;
}
function getMarketsAndPriceFeeds(
    marginAccount,
    markets
) {
    const marketAddresses = [];
    const priceFeedAddresses = [];
    for (const position of marginAccount.positions()) {
        const market = markets[ position.marketId() ];
        if (market.address === undefined) {
            throw new Error(`Market is missing from markets map (id=${position.marketId()})`);
        }
        marketAddresses.push(market.address);
        priceFeedAddresses.push(market.priceFeed());
    }
    return [ marketAddresses, priceFeedAddresses ];
}
async function liquidate(
    sdk,
    connection,
    marginAccount,
    accounts,
    markets,
    signers,
    feePayer,
    params
) {
    const [ marketAddresses, priceFeedAddresses ] = getMarketsAndPriceFeeds(marginAccount, markets);
    const blockhash = await connection.getLatestBlockhash();
    const tx = sdk
        .transactionBuilder()
        .liquidate(accounts, marketAddresses, priceFeedAddresses, params)
        .feePayer(feePayer)
        .buildSigned(signers, blockhash.blockhash);
    return await sendTransaction(connection, tx, blockhash, signers);
}


const wait = async (ms) => {
    return new Promise((resolve) => setTimeout(resolve, ms));
};
const SEND_OPTIONS = {
    skipPreflight: true,
};

async function sendTransaction(
    connection,
    transaction,
    blockhashWithExpiryBlockHeight,
    signers,
) {
    // increase compute fee
    transaction.add(
        ComputeBudgetProgram.setComputeUnitPrice({
            microLamports: 10000
        })
    );
    transaction.sign(...signers);
    const txid = await connection.sendRawTransaction(
        transaction.serialize(),
        SEND_OPTIONS
    );

    const controller = new AbortController();
    const abortSignal = controller.signal;

    const abortableResender = async () => {
        while (true) {
            await wait(2_000);
            if (abortSignal.aborted) return;
            try {
                await connection.sendRawTransaction(
                    transaction.serialize(),
                    SEND_OPTIONS
                );
            } catch (e) {
                console.warn(`Failed to resend transaction: ${e}`);
            }
        }
    };

    try {
        abortableResender();
        const lastValidBlockHeight =
            blockhashWithExpiryBlockHeight.lastValidBlockHeight - 150;

        await Promise.race([
            connection.confirmTransaction(
                {
                    ...blockhashWithExpiryBlockHeight,
                    lastValidBlockHeight,
                    signature: txid,
                    abortSignal,
                },
                "confirmed"
            ),
            new Promise((resolve) => {
                while (!abortSignal.aborted) {
                    wait(2000).then(async () => {
                        const tx = await connection.getSignatureStatus(txid, {
                            searchTransactionHistory: false,
                        });
                        if (tx?.value?.confirmationStatus === "confirmed") {
                            resolve(tx);
                        }
                    });
                }
            }),
        ]);
    } catch (e) {
        if (e instanceof TransactionExpiredBlockheightExceededError) {
            // useless error
            return null;
        } else {
            throw e;
        }
    } finally {
        controller.abort();
    }

    // in case rpc is not synced yet, we add some retries
    const response = promiseRetry(
        async (retry) => {
            const response = await connection.getTransaction(txid, {
                commitment: "confirmed",
                maxSupportedTransactionVersion: 0,
            });
            if (!response) {
                retry(response);
            }
            return response;
        },
        {
            retries: 5,
            minTimeout: 1e3,
        }
    );

    return response;
}