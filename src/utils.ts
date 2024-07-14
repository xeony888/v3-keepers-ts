import { Exchange, ExchangeWrapper, MarginAccountWrapper, MarginsWrapper, Market, MarketMap, MarketWrapper, ParclV3Sdk, PriceFeedMap, ProgramAccount } from "@parcl-oss/v3-sdk";
import { PublicKey } from "@solana/web3.js";


export async function timer(f: () => any) {
    const start = Date.now();
    const result = await f();
    const time = Date.now() - start;
    return { result, time };
}

export function now_seconds() {
    return Math.floor(Date.now() / 1000);
}

export function distanceToLiquidation(m: MarginsWrapper) {
    return m.margins.availableMargin.sub(m.totalRequiredMargin());
}

export function isUsed(m: MarginsWrapper) {
    for (const key in m.margins) {
        // @ts-ignore
        if (m.margins[key].val.eq(0)) {
            return false;
        }
    }
    return true;
}

export async function retrieveAllActiveMarginAccounts(sdk: ParclV3Sdk, exchange: Exchange, exchangeWrapper: ExchangeWrapper, allMarketAddresses: PublicKey[]) {
    const allMarkets = await sdk.accountFetcher.getMarkets(allMarketAddresses);
    const [markets, priceFeeds] = await getMarketMapAndPriceFeedMap(sdk, allMarkets);
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
    }).sort((a, b) => a.distance.sub(b.distance).val.toNumber()) as any;

    console.log(`Active margin accounts: ${margins.length}`);
    return margins;
}

export async function getMarketMapAndPriceFeedMap(
    sdk: ParclV3Sdk,
    allMarkets: (ProgramAccount<Market> | undefined)[]
): Promise<[MarketMap, PriceFeedMap]> {
    const markets: MarketMap = {};
    for (const market of allMarkets) {
        if (market === undefined) {
            continue;
        }
        markets[market.account.id] = new MarketWrapper(market.account, market.address);
    }
    const allPriceFeedAddresses = (allMarkets as ProgramAccount<Market>[]).map(
        (market) => market.account.priceFeed
    );
    const allPriceFeeds = await sdk.accountFetcher.getPythPriceFeeds(allPriceFeedAddresses);
    const priceFeeds: PriceFeedMap = {};
    for (let i = 0; i < allPriceFeeds.length; i++) {
        const priceFeed = allPriceFeeds[i];
        if (priceFeed === undefined) {
            continue;
        }
        priceFeeds[allPriceFeedAddresses[i]] = priceFeed;
    }
    return [markets, priceFeeds];
}