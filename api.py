import csv
import logging
import os
import time
from collections import deque

import numpy as np
from binance.lib.utils import config_logging
from binance.websocket.um_futures.websocket_client import \
    UMFuturesWebsocketClient

import toolsec

# 配置日志记录设置
config_logging(logging, logging.DEBUG)

class TradeData:
    def __init__(self):
        self.trades = deque()
        self.total_buy_price_volume = 0
        self.total_sell_price_volume = 0
        self.total_buy_volume = 0
        self.total_sell_volume = 0

    def add_trade(self, trade):
        self.trades.append(trade)
        price = float(trade[1])
        volume = float(trade[2])
        if trade[3]:  # 卖单
            self.total_sell_price_volume += price * volume
            self.total_sell_volume += volume
        else:  # 买单
            self.total_buy_price_volume += price * volume
            self.total_buy_volume += volume

    def remove_old_trades(self, current_time):
        while self.trades and current_time - self.trades[0][0] > 60000:
            trade = self.trades.popleft()
            price = float(trade[1])
            volume = float(trade[2])
            if trade[3]:  # 卖单
                self.total_sell_price_volume -= price * volume
                self.total_sell_volume -= volume
            else:  # 买单
                self.total_buy_price_volume -= price * volume
                self.total_buy_volume -= volume

    def get_avg_buy_price(self):
        return self.total_buy_price_volume / self.total_buy_volume if self.total_buy_volume else 0

    def get_avg_sell_price(self):
        return self.total_sell_price_volume / self.total_sell_volume if self.total_sell_volume else 0

    def get_total_buy_volume(self):
        return self.total_buy_volume

    def get_total_sell_volume(self):
        return self.total_sell_volume

class BinanceWebSocketClient:
    def __init__(self, symbol, depth_level=None, update_speed=None, is_combined=False, is_debug=False, data_saver_depthupdate=None):
        self._symbol = symbol
        self._depth_level = depth_level
        self._update_speed = update_speed
        self._is_combined = is_combined
        self._is_debug_flag = is_debug
        self._agg_trade_data = TradeData()  # 使用自定义的TradeData类
        self.starttime = time.time()
        self.data_saver_depthupdate = data_saver_depthupdate
        proxies = {'https': 'http://127.0.0.1:7890'}
        if is_debug:
            self._websocket_client = UMFuturesWebsocketClient(on_message=self._on_message_debug, is_combined=self._is_combined, proxies=proxies)
        else:
            self._websocket_client = UMFuturesWebsocketClient(on_message=self._data, is_combined=self._is_combined, proxies=proxies)

    def _on_message_debug(self, process_id, message):
        logging.info("收到来自 %s 的消息: %s", process_id, message)

    def _data(self, process_id, message):
        data, data_type, time_re = toolsec.process_json_to_list(message)
        if data_type == 'aggTrade':
            self._agg_trade_data.add_trade(data)
            self._agg_trade_data.remove_old_trades(time_re)
        elif data_type == 'depthUpdate' and time.time()-self.starttime>60:
            self._process_depth_update(data)

    def _process_depth_update(self, data):
        avg_buy_price = self._agg_trade_data.get_avg_buy_price()
        avg_sell_price = self._agg_trade_data.get_avg_sell_price()
        total_buy_volume = self._agg_trade_data.get_total_buy_volume()
        total_sell_volume = self._agg_trade_data.get_total_sell_volume()

        # 将计算结果写入 data_saver_depthupdate
        self.data_saver_depthupdate.add_data(data+[avg_buy_price,avg_sell_price,total_buy_volume,total_sell_volume])

    def start_depth_subscription(self):
        self._websocket_client.partial_book_depth(
            symbol=self._symbol,
            id=1,
            level=self._depth_level,
            speed=self._update_speed,
        )
        logging.info("已开始订阅深度数据 %s", self._symbol)

    def start_agg_trade_subscription(self):
        self._websocket_client.agg_trade(symbol=self._symbol)
        logging.info("已开始订阅聚合交易流 %s", self._symbol)

    def stop_depth_subscription(self):
        self._websocket_client.partial_book_depth(
            symbol=self._symbol,
            id=1,
            level=self._depth_level,
            speed=self._update_speed,
            action=UMFuturesWebsocketClient.ACTION_UNSUBSCRIBE
        )
        logging.info("已取消订阅深度数据 %s", self._symbol)

    def stop_agg_trade_subscription(self):
        self._websocket_client.agg_trade(
            symbol=self._symbol,
            action=UMFuturesWebsocketClient.ACTION_UNSUBSCRIBE
        )
        logging.info("已取消订阅聚合交易流 %s", self._symbol)

    def close_connection(self):
        self._websocket_client.stop()
        self.data_saver_aggtrade.stop()
        self.data_saver_depthupdate.stop()
        logging.debug("已关闭WebSocket连接")



if __name__ == "__main__":
    columns1 = ["e", "E", "T", "s", "U", "u", "pu", 
                "b1", "b1v", "b2", "b2v", "b3", "b3v", "b4", "b4v", "b5", "b5v",
                "b6", "b6v", "b7", "b7v", "b8", "b8v", "b9", "b9v", "b10", "b10v",
                "a1", "a1v", "a2", "a2v", "a3", "a3v", "a4", "a4v", "a5", "a5v", 
                "a6", "a6v", "a7", "a7v", "a8", "a8v", "a9", "a9v", "a10", "a10v",
                "b1m","a1m","bv1m","av1m"
                ]

    data_saver_depthupdate = toolsec.DataSaver("data1.csv", columns1, interval=10)


    # 创建客户端实例并开始订阅
    client = BinanceWebSocketClient(symbol="neirousdt", depth_level=10, update_speed=100,is_debug=0,
                                    data_saver_depthupdate=data_saver_depthupdate,)
    client.start_depth_subscription()
    client.start_agg_trade_subscription()
    time.sleep(36000)  # 保持连接10秒钟

    # 停止深度数据订阅
    client.stop_depth_subscription()
    # 停止聚合交易流订阅
    client.stop_agg_trade_subscription()
    
    # 关闭连接
    logging.info("关闭WebSocket连接")
    client.close_connection()
