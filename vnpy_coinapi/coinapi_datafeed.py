from datetime import datetime
from typing import Optional, List, Callable
import pytz

import ccxt

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.object import BarData, HistoryRequest, TickData
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.locale import _

# 时间周期映射: vnpy Interval -> CCXT timeframe
# vnpy 4.3 的 Interval 定义:
#   MINUTE = "1m", HOUR = "1h", DAILY = "d", WEEKLY = "w", TICK = "tick"
INTERVAL_VT2CCXT = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",   # vnpy是 "d"，CCXT是 "1d"
    Interval.WEEKLY: "1w",  # vnpy是 "w"，CCXT是 "1w"
}

# 时间周期对应的秒数
INTERVAL_SECONDS = {
    "1m": 60,
    "1h": 3600,
    "1d": 86400,
    "1w": 604800,
}


class CoinapiDatafeed(BaseDatafeed):
    """CCXT数据服务接口 - 支持币安U本位合约"""

    def __init__(self):
        """初始化 CCXT 币安U本位合约接口"""
        # 配置代理（如果需要）
        # 可以通过环境变量或SETTINGS配置代理
        proxy_url = SETTINGS.get("datafeed.proxy", "")
        
        # 默认使用本地代理 127.0.0.1:10809
        if not proxy_url:
            proxy_url = "http://127.0.0.1:10809"
        
        # 创建交易所实例 - 获取历史行情不需要 API Key
        config = {
            'enableRateLimit': True,  # 启用自动限流，避免IP被封
        }
        
        # 如果配置了代理，添加代理设置
        if proxy_url:
            config['proxies'] = {
                'http': proxy_url,
                'https': proxy_url,
            }
        
        self.exchange = ccxt.binanceusdm(config)
        
        # 可选：保存API Key用于后续扩展功能
        self.api_key: str = SETTINGS.get("datafeed.username", "")
        self.secret_key: str = SETTINGS.get("datafeed.password", "")

    def init(self, output: Callable = print) -> bool:
        """
        初始化数据服务连接（vnpy 4.3 新增方法）
        
        Args:
            output: 日志输出函数
            
        Returns:
            bool: 初始化是否成功
        """
        try:
            # 加载市场信息
            self.exchange.load_markets()
            
            # 如果配置了API Key，则启用私有接口
            if self.api_key and self.secret_key:
                self.exchange.apiKey = self.api_key
                self.exchange.secret = self.secret_key
                output(_("CCXT数据服务已初始化（带认证）"))
            else:
                output(_("CCXT数据服务已初始化（无认证，仅支持公共数据）"))
            
            return True
        except Exception as e:
            output(_("CCXT数据服务初始化失败：{}").format(str(e)))
            return False

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData]:
        """
        查询K线历史数据（适配 vnpy 4.3）
        
        Args:
            req: 历史数据请求
            output: 日志输出函数
            
        Returns:
            list[BarData]: K线数据列表
        """
        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        timestart: float = req.start.timestamp()
        timeend: float = req.end.timestamp()

        # ------------------- 检查参数 ----------------------------------------
        if exchange.value != "BINANCE":
            output(_("查询K线数据失败：交易所 {} 不受支持").format(exchange.value))
            return []

        # 时间周期映射
        if interval not in INTERVAL_VT2CCXT:
            output(_("查询K线数据失败：时间周期 {} 不受支持").format(interval.value))
            return []

        timeframe: str = INTERVAL_VT2CCXT[interval]
        interval_seconds: int = INTERVAL_SECONDS[timeframe]

        # ------------------- 计算数量 ----------------------------------------
        count = int(timeend - timestart)
        if timeframe == "1m":
            count = count // 60
        elif timeframe == "1h":
            count = count // 3600
        elif timeframe == "1d":
            count = count // 86400
        elif timeframe == "1w":
            count = count // 604800

        output(_("查询 {} {} K线数据: {} 到 {}, 预计 {} 条").format(
            symbol,
            timeframe,
            datetime.fromtimestamp(timestart),
            datetime.fromtimestamp(timeend),
            count
        ))

        # ------------------- 查询 CCXT 接口 ----------------------------------------
        # 转换 symbol 格式: ethusdt -> ETH/USDT
        ccxt_symbol = self._convert_symbol(symbol)

        bars: list[BarData] = []
        current_since: int = int(timestart * 1000)  # CCXT 使用毫秒时间戳
        max_end_time: int = int(timeend * 1000)

        while current_since < max_end_time:
            try:
                # 获取K线数据
                ohlcv = self.exchange.fetch_ohlcv(
                    symbol=ccxt_symbol,
                    timeframe=timeframe,
                    since=current_since,
                    limit=1000  # 币安最大支持1000条
                )

                if not ohlcv:
                    break

                # 转换为 BarData
                for candle in ohlcv:
                    timestamp_ms, open_p, high_p, low_p, close_p, volume = candle
                    timestamp = timestamp_ms / 1000

                    # 检查是否超出结束时间
                    if timestamp > timeend:
                        break

                    dt = pytz.utc.localize(datetime.fromtimestamp(timestamp))

                    bar = BarData(
                        symbol=symbol,
                        exchange=exchange,
                        interval=interval,
                        datetime=dt,
                        open_price=open_p,
                        high_price=high_p,
                        low_price=low_p,
                        close_price=close_p,
                        volume=volume,
                        gateway_name="CA",
                    )
                    bars.append(bar)

                # 更新下次查询的起始时间
                last_candle = ohlcv[-1]
                current_since = last_candle[0] + interval_seconds * 1000

                # 如果返回数据不足1000条，说明已获取完毕
                if len(ohlcv) < 1000:
                    break

            except Exception as e:
                output(_("查询K线数据失败：{}").format(str(e)))
                break

        output(_("查询完成，共获取 {} 条K线数据").format(len(bars)))
        return bars

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData]:
        """
        查询Tick历史数据（vnpy 4.3 新增方法）
        
        Args:
            req: 历史数据请求
            output: 日志输出函数
            
        Returns:
            list[TickData]: Tick数据列表
        """
        output(_("查询Tick数据失败：当前数据服务不支持Tick数据"))
        return []

    def _convert_symbol(self, symbol: str) -> str:
        """
        将 vnpy symbol 转换为 CCXT symbol 格式
        
        币安U本位合约的格式为: ETH/USDT:USDT (线性合约)
        
        Args:
            symbol: vnpy格式的交易对，如 ethusdt
            
        Returns:
            str: CCXT格式的交易对，如 ETH/USDT:USDT
        """
        symbol = symbol.upper()
        
        # 如果已经是CCXT格式（包含:），直接返回
        if ':' in symbol:
            return symbol
        
        # 如果包含/但不包含:，添加结算货币
        if '/' in symbol and ':' not in symbol:
            # 从 ETH/USDT 转换为 ETH/USDT:USDT
            quote = symbol.split('/')[1]
            return f"{symbol}:{quote}"
        
        # 处理常见的稳定币后缀（按长度降序，避免USDT被USD截断）
        stable_coins = ['USDT', 'USDC', 'BUSD', 'TUSD', 'DAI']
        
        for coin in stable_coins:
            if symbol.endswith(coin):
                base = symbol[:-len(coin)]
                # 币安U本位合约格式: ETH/USDT:USDT
                return f"{base}/{coin}:{coin}"
        
        # 如果没有匹配到稳定币，尝试按USD分割
        if symbol.endswith('USD'):
            base = symbol[:-3]
            return f"{base}/USD:USD"
        
        # 默认返回原样
        return symbol


if __name__ == "__main__":
    # 简单测试
    datafeed = CoinapiDatafeed()
    datafeed.init()
    
    from datetime import datetime
    
    req = HistoryRequest(
        symbol="ethusdt",
        exchange=Exchange.BINANCE,
        interval=Interval.DAILY,
        start=datetime(2024, 1, 1),
        end=datetime(2024, 1, 5)
    )
    
    bars = datafeed.query_bar_history(req)
    print(f"获取到 {len(bars)} 条数据")
    if bars:
        print(f"第一条: {bars[0]}")
        print(f"最后一条: {bars[-1]}")
