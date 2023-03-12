import config
from binance.client import AsyncClient
from time import sleep
from datetime import date, datetime
import pandas as pd
import json
import asyncio
import os


class SpotFutureArb:

    MAIN_TICKERS = ["BTCUSD", "ETHUSD"]
    
    def __init__(self, symbols: list, expired_date: date, treshold: float) -> None:
        # call async init
        self.symbols: list = symbols
        self.expired_date: date = expired_date
        self.treshold: float = treshold
        module_dir = os.path.dirname(__file__)
        self.data_folder = os.path.join(module_dir, "data")
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)

        try:
            asyncio.run(self._async_init())
        except Exception as e:
            print(f"[FATAL ERROR] This should not trigger {e}")

    async def _async_init(self):
        self.client = await AsyncClient.create(config.BinanceAPIKey, config.BinanceAPISecretKey)
        self.spread_dfs: dict = {}
        for symbol in self.symbols:
            self.spread_dfs[symbol] = pd.DataFrame(columns=["spot", f"coin_{self.expired_date}", "coin_perp", f"usd_{self.expired_date}", "usd_perp"])

        while True:
            try:
                for symbol in self.symbols:
                    await self.load_single_symbol(symbol)
                    self.spread_dfs[symbol].to_csv(f"{self.data_folder}/{symbol}.csv")
            except KeyboardInterrupt:
                self.client.close_connection()
                print("[WARNING] KeyboardInterrupt exiting...")
                break
            except Exception as e:
                print(f"[ERROR] {e}")
                self.client.close_connection()
                break

    async def load_single_symbol(self, symbol: str) -> float:
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async def return_none():
                return None

            calls = [
                self.client.get_recent_trades(symbol=symbol+"T"),
                self.client.futures_coin_symbol_ticker(symbol=f"{symbol}_{expired_date}"),
                self.client.futures_coin_symbol_ticker(symbol=f"{symbol}_PERP"),
                self.client.futures_symbol_ticker(symbol=f"{symbol}T_{expired_date}") if symbol in self.MAIN_TICKERS else return_none(),
                self.client.futures_symbol_ticker(symbol=f"{symbol}T")
            ]

            raw = await asyncio.gather(*calls)
            spot, future, future_perp, future_usd, future_usd_perp = raw

            spot_price = float(spot[0]["price"]) if spot else None
            future_price = float(future[0]["price"]) if future else None
            future_perp_price = float(future_perp[0]["price"]) if future_perp else None
            future_usd_price = float(future_usd["price"]) if future_usd else None
            future_usd_perp_price = float(future_usd_perp["price"]) if future_usd_perp else None

            self.spread_dfs[symbol].loc[now, "spot"] = spot_price
            self.spread_dfs[symbol].loc[now, f"coin_{self.expired_date}"] = future_price
            self.spread_dfs[symbol].loc[now, "coin_perp"] = future_perp_price
            self.spread_dfs[symbol].loc[now, f"usd_{self.expired_date}"] = future_usd_price
            self.spread_dfs[symbol].loc[now, "usd_perp"] = future_usd_perp_price
            
            print(f"{symbol} - {now} OK")

        except KeyboardInterrupt:
            print("[WARNING] KeyboardInterrupt")
            raise KeyboardInterrupt
        
        except Exception as e:
            print(f"[ERROR] {e}")


expired_date = date(2023, 3, 31).strftime("%y%m%d")
treshold = 10

tickers = ["BTCUSD", "ETHUSD", "XRPUSD", "ADAUSD", "BCHUSD", "LINKUSD", "DOTUSD"]

arb = SpotFutureArb(tickers, expired_date, treshold)
