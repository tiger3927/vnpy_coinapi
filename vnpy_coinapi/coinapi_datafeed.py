from datetime import datetime
import dateutil, time
from dateutil import parser
from typing import Optional, List
import requests
import pytz
import json

from binance_f import RequestClient
from binance_f import SubscriptionClient
from binance_f.model import Candlestick
from binance_f.model import *
from binance_f.constant.test import *
from binance_f.base.printobject import *
from binance_f.exception.binanceapiexception import BinanceApiException

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.constant import Exchange, Interval

INTERVAL_VT2CA = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
    Interval.TICK: ""
}

g_api_key = ""
g_secret_key = ""

request_client: RequestClient = None
sub_client: SubscriptionClient = None


class CoinapiDatafeed(BaseDatafeed):
    """CoinAPI数据服务接口"""

    def __init__(self):
        global g_api_key, g_secret_key

        self.api_key: str = SETTINGS.get("datafeed.username", g_api_key)
        self.secret_key: str = SETTINGS.get("datafeed.password", g_secret_key)

        #if SETTINGS["datafeed.name"] != "coinapi":
        self.api_key = ""
        self.secret_key = ""

        self.request_client = RequestClient(api_key=self.api_key, secret_key=self.secret_key)

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询k线数据"""
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        timestart = req.start.timestamp()
        timeend = req.end.timestamp()

        # timestart = float(int(timestart))  # 取整 按秒钟
        # timeend = float(int(timeend))

        # ------------------- 检查 参数 ----------------------------------------

        if exchange.value != "BINANCE":
            print("query_bar_history binance u 合约: ", "exchange not support")
            return None

        period_id = interval.value
        if period_id == "d":
            period_id = "1d"

        if period_id not in ["1m", "1h", "1d"]:
            print("query_bar_history binance u 合约: ", "interval not support")
            return None

        # ------------------- 计算 数量 ----------------------------------------

        count = int(timeend - timestart)
        interseccount = 0
        if period_id == "1m":
            count = count // 60
            interseccount = 60
        elif period_id == "1h":
            count = count // 3600
            interseccount = 3600
        elif period_id == "1d":
            count = count // 86400
            interseccount = 86400
        else:
            print("query_bar_history binance u 合约: ", "interval not support")
            return None
        print("query_bar_history binance u 合约: ", datetime.fromtimestamp(timestart), datetime.fromtimestamp(timeend),
              count, interseccount)

        # ------------------- 查询 接口 ----------------------------------------

        i = 0
        rs = []
        while (i < count):
            looptimestart = timestart + i * interseccount * 1.0
            if i + 1000 < count:
                looptimeend = timestart + i * interseccount * 1.0 + interseccount * 1000.0
                loopcount = 1000
            else:
                looptimeend = timeend
                loopcount = count - i
            # print("query_bar_history binance u 合约 loop : ", looptimestart, looptimeend)
            # print("query_bar_history binance u 合约 loop : ", datetime.fromtimestamp(looptimestart),datetime.fromtimestamp(looptimeend))
            result = self.request_client.get_candlestick_data(symbol=symbol, interval=period_id, limit=loopcount,
                                                              startTime=looptimestart * 1000,
                                                              endTime=looptimeend * 1000)
            # print("query_bar_history binance u 合约 loop result count: ", len(result))
            rs += result

            i = i + 1000
            if i >= count:
                break

        bars: List[BarData] = []
        for o in rs:
            t = o.closeTime / 1000.0
            # print("+++++",t,timestart,timeend)
            # print("-----------",datetime.fromtimestamp(t),datetime.fromtimestamp(timestart),datetime.fromtimestamp(timeend))
            dt = pytz.utc.localize(datetime.fromtimestamp(t))
            if t > timeend:
                break
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                datetime=dt,
                open_price=o.open,
                high_price=o.high,
                low_price=o.low,
                close_price=o.close,
                volume=o.volume,
                turnover=o.numTrades,
                open_interest=o.takerBuyBaseAssetVolume,
                gateway_name="CA",
            )
            bars.append(bar)
        print("query_bar_history binance u 合约 bars count: ", len(bars))
        return bars


if __name__ == "__main__":

    datafeed = CoinapiDatafeed()
    r = HistoryRequest(symbol="ethusdt", exchange=Exchange.BINANCE, interval=Interval.DAILY,
                       start=datetime(2021, 1, 1, 1, 0, 0), end=datetime(2021, 1, 2, 3, 0, 0))
    r = datafeed.query_bar_history(r)
    print("result : ", len(r))

