"""Resume Binance Futures trade history through CryptoHFTData."""

import asyncio

from dccd import Client


async def main() -> None:
    """Download the latest bounded trade window into dccd's Parquet store."""
    async with Client() as client:
        result = await client.backfill(
            "cryptohftdata-binance-futures",
            "BTC/USDT:perp",
            data_type="trades",
            start="last",
        )
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
