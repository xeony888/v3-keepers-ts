import {
  ProgramAccount,
  Market,
  ParclV3Sdk,
  getExchangePda,
  getMarketPda,
  MarginAccountWrapper,
  MarketWrapper,
  ExchangeWrapper,
  LiquidateAccounts,
  LiquidateParams,
  MarketMap,
  PriceFeedMap,
  Address,
  translateAddress,
} from "@parcl-oss/v3-sdk";
import {
  Commitment,
  Connection,
  Keypair,
  PublicKey,
  Signer,
} from "@solana/web3.js";
import bs58 from "bs58";
import * as dotenv from "dotenv";
import { sendTransaction } from "./sender";
import { distanceToLiquidation, getMarketMapAndPriceFeedMap, isUsed, now_seconds, retrieveAllActiveMarginAccounts, timer } from "./utils";
import { isMainThread, parentPort, Worker, workerData } from "worker_threads";
import { createClient } from "redis";
dotenv.config();

if (!process.env.RPC_URL) {
  throw new Error("Missing rpc url");
}
if (!process.env.LIQUIDATOR_MARGIN_ACCOUNT) {
  throw new Error("Missing liquidator margin account");
}
if (!process.env.PRIVATE_KEY) {
  throw new Error("Missing liquidator signer");
}
const THREAD_COUNT: number = 6; // change if your computer has more threads
(async function main() {
  // const [exchangeAddress] = getExchangePda(0);
  // const commitment = process.env.COMMITMENT as Commitment | undefined;
  // const connection = new Connection(process.env.RPC_URL!);
  // const sdk = new ParclV3Sdk({ rpcUrl: process.env.RPC_URL!, commitment });
  // const liquidatorMarginAccount = translateAddress(process.env.LIQUIDATOR_MARGIN_ACCOUNT!);
  // const liquidatorSigner = Keypair.fromSecretKey(bs58.decode(process.env.PRIVATE_KEY!));
  // const interval = parseInt(process.env.INTERVAL ?? "300");
  // runLiquidator({
  //   sdk, connection, interval, exchangeAddress, liquidatorMarginAccount, liquidatorSigner
  // });
  // return;
  const redis = createClient();
  await redis.connect();
  await redis.del("activeMarginAccounts"); // clean up older accounts when start
  if (isMainThread) {
    console.log("Starting liquidator (main)");
    // Note: only handling single exchange

    // const exchange = await sdk.accountFetcher.getExchange(exchangeAddress);
    // if (!exchange) throw new Error("Could not get exchange");
    // const exchangeWrapper = new ExchangeWrapper(exchange);
    // const allMarketAddresses: PublicKey[] = exchange.marketIds.filter(marketId => marketId != 0).map(marketId => getMarketPda(exchangeAddress, marketId)[0]);
    // let activeMarginAccounts = await retrieveAllActiveMarginAccounts(sdk, exchange, exchangeWrapper, allMarketAddresses);

    // spawns a worker to periodically get all margin accounts
    const worker1 = new Worker("./src/worker.js", { workerData: { task: "activeMarginAccounts" } });
    // spawn 10 seperate threads, each responsible for 
    let start: number = 0;
    let end: number = 0;
    for (let i = 0; i < THREAD_COUNT; i++) {
      // 2^i+2 threads
      if (i == THREAD_COUNT - 1) {
        start = end;
        end = Infinity;
      } else {
        const temp = end;
        start = temp;
        end = temp + 2 ** (i + 2);
      }
      // spawns workers of different priority levels. The higher a worker's priority the more often it checks accounts closer to liquidation
      const w = new Worker("./src/worker.js", { workerData: { task: "accountCheckAndLiquidate", slice: [start, end], interval: 10 * 2 ** i } });
    }
  }
})();

type RunLiquidatorParams = {
  sdk: ParclV3Sdk;
  connection: Connection;
  interval: number;
  exchangeAddress: Address;
  liquidatorSigner: Keypair;
  liquidatorMarginAccount: Address;
};

async function runLiquidator({
  sdk,
  connection,
  interval,
  exchangeAddress,
  liquidatorSigner,
  liquidatorMarginAccount,
}: RunLiquidatorParams): Promise<void> {
  const exchange = await sdk.accountFetcher.getExchange(exchangeAddress);
  if (!exchange) throw new Error("Could not get exchange");

  let firstRun = true;
  while (true) {
    if (firstRun) {
      firstRun = false;
    } else {
      await new Promise((resolve) => setTimeout(resolve, interval * 1000));
    }
    const exchange = await sdk.accountFetcher.getExchange(exchangeAddress);
    if (exchange === undefined) {
      throw new Error("Invalid exchange address");
    }
    const allMarketAddresses: PublicKey[] = [];
    for (const marketId of exchange!.marketIds) {
      if (marketId === 0) {
        continue;
      }
      const [market] = getMarketPda(exchangeAddress, marketId);
      allMarketAddresses.push(market);
    }
    const allMarkets = await sdk.accountFetcher.getMarkets(allMarketAddresses);
    const { result: [[markets, priceFeeds], allMarginAccounts], time } = await timer(async () => await Promise.all([
      getMarketMapAndPriceFeedMap(sdk, allMarkets),
      sdk.accountFetcher.getAllMarginAccounts(),
    ]));
    console.log(`Fetched ${allMarginAccounts.length} margin accounts in ${time / 1000}s`);
    for (const rawMarginAccount of allMarginAccounts) {
      const marginAccount = new MarginAccountWrapper(
        rawMarginAccount.account,
        rawMarginAccount.address
      );
      if (true) { //(marginAccount.inLiquidation()) {
        console.log(`Liquidating account already in liquidation (${marginAccount.address})`);
        await liquidate(
          sdk,
          connection,
          marginAccount,
          {
            marginAccount: rawMarginAccount.address,
            exchange: rawMarginAccount.account.exchange,
            owner: rawMarginAccount.account.owner,
            liquidator: liquidatorSigner.publicKey,
            liquidatorMarginAccount,
          },
          markets,
          [liquidatorSigner],
          liquidatorSigner.publicKey
        );
      }
      const margins = marginAccount.getAccountMargins(
        new ExchangeWrapper(exchange!),
        markets,
        priceFeeds,
        Math.floor(Date.now() / 1000)
      );
      if (margins.canLiquidate()) {
        console.log(`Starting liquidation for ${marginAccount.address}`);
        const signature = await liquidate(
          sdk,
          connection,
          marginAccount,
          {
            marginAccount: rawMarginAccount.address,
            exchange: rawMarginAccount.account.exchange,
            owner: rawMarginAccount.account.owner,
            liquidator: liquidatorSigner.publicKey,
            liquidatorMarginAccount,
          },
          markets,
          [liquidatorSigner],
          liquidatorSigner.publicKey
        );
        console.log("Signature: ", JSON.stringify(signature));
      }
    }
  }
}


function getMarketsAndPriceFeeds(
  marginAccount: MarginAccountWrapper,
  markets: MarketMap
): [Address[], Address[]] {
  const marketAddresses: Address[] = [];
  const priceFeedAddresses: Address[] = [];
  for (const position of marginAccount.positions()) {
    const market = markets[position.marketId()];
    if (market.address === undefined) {
      throw new Error(`Market is missing from markets map (id=${position.marketId()})`);
    }
    marketAddresses.push(market.address);
    priceFeedAddresses.push(market.priceFeed());
  }
  return [marketAddresses, priceFeedAddresses];
}

async function liquidate(
  sdk: ParclV3Sdk,
  connection: Connection,
  marginAccount: MarginAccountWrapper,
  accounts: LiquidateAccounts,
  markets: MarketMap,
  signers: Signer[],
  feePayer: Address,
  params?: LiquidateParams
) {
  const [marketAddresses, priceFeedAddresses] = getMarketsAndPriceFeeds(marginAccount, markets);
  const blockhash = await connection.getLatestBlockhash();
  const tx = sdk
    .transactionBuilder()
    .liquidate(accounts, marketAddresses, priceFeedAddresses, params)
    .feePayer(feePayer).buildUnsigned();
  // .buildSigned(signers, blockhash.blockhash); we are adding more instructions, so we build unsigned for now
  return await sendTransaction(connection, tx, blockhash, signers);
}
