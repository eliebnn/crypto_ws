import datetime as dt

from crypto_ws.core_ws import CoreWS


class BybitWS(CoreWS):

    CACHING_KEY = 'default_redis_caching_key:bybit'
    PUBLISH_CHANNEL = 'default_redis_publish_key:bybit'

    CHANNELS = ['publicTrade', 'tickers', 'orderbook.1', 'orderbook.50',
                'kline.1', 'kline.3', 'kline.5', 'kline.15', 'kline.30', 'kline.60', 'kline.120', 'kline.240',
                'kline.360', 'kline.720', 'kline.D', 'kline.W', 'kline.M']

    def __init__(self, url='wss://stream.bybit.com/v5/public/spot', markets=('BTCUSDT', 'ETHUSDT'),
                 channels=('tickers',), caching_freq=0.25, translate=None, redis_kwargs=None, **kwargs):

        super().__init__(url, markets, channels, caching_freq, translate, redis_kwargs, **kwargs)

    # ----

    def _keep_alive(self):
        self._send({'op': 'ping'})

    # ----

    def _subscribe(self):

        for m in self.markets:
            for c in self.channels:
                payload = {"op": "subscribe", "args": [f"{c}.{m}"]}
                self._send(payload)

    def _loop(self):
        while True:

            data = self._rcv()
            self._heart_beat()

            if isinstance(data, dict) and 'data' in data.keys():

                _ = data['topic'].split('.')
                channel = '.'.join(_[:-1])
                market = self._do_translate(_[-1])

                if 'tickers' in channel:
                    msg = TickerParser.parse(data, subset=None)

                elif 'kline.' in channel:
                    msg = BarParser.parse(data, subset=None)

                elif 'orderbook.' in channel:
                    msg = DepthParser.parse(data, subset=None)

                elif 'publicTrade' in channel:
                    msg = TradeParser.parse(data, subset=None)

                else:
                    continue

                if not msg:
                    continue

                print({market: msg}) if self.verbose > 0 else None

                self.results[channel].update({market: msg})
                self._publish(channel, {market: msg})
                self._cache()


class Parser:
    @staticmethod
    def parse_datetime(x):
        return dt.datetime.utcfromtimestamp(int(str(x)[:-3])).strftime('%Y-%m-%d %H:%M:%S.%MS')


class TickerParser:

    map = {
        'symbol': ['symbol', str],
        'lastPrice': ['last_price', float],
        'highPrice24h': ['high_price_24h', float],
        'lowPrice24h': ['low_price_24h', float],
        'prevPrice24h': ['prev_price_24h', float],
        'volume24h': ['volume_24h', float],
        'turnover24h': ['turnover_24h', float],
        'price24hPcnt': ['price_pct_24h', float],
        'usdIndexPrice': ['usd_index_price', float],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in TickerParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['data'].items()
               if (key_value_pair := TickerParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


class BarParser:
    map = {
        'start': ['start', Parser.parse_datetime],
        'end': ['end', Parser.parse_datetime],
        'interval': ['interval', str],
        'open': ['open', float],
        'close': ['close', float],
        'high': ['high', float],
        'low': ['low', float],
        'volume': ['volume', float],
        'turnover': ['turnover', float],
        'confirm': ['confirm', bool],
        'timestamp': ['timestamp', Parser.parse_datetime],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in BarParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['data'][0].items()
               if (key_value_pair := BarParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


class DepthParser:
    map = {
        0: ['price', float],
        1: ['volume', float],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in DepthParser.map.values()]

        if msg['data']['u'] == 1:
            return None

        asks = [{key_value_pair[0]: key_value_pair[1](k) for idx, k in enumerate(_)
                if (key_value_pair := DepthParser.map.get(idx, idx))[0] in subset} for _ in msg['data']['a']]

        bids = [{key_value_pair[0]: key_value_pair[1](k) for idx, k in enumerate(_)
                if (key_value_pair := DepthParser.map.get(idx, idx))[0] in subset} for _ in msg['data']['b']]

        dct = {'bids': bids, 'asks': asks, 'respond_time_utc': Parser.parse_datetime(msg['ts'])}

        return dct


class TradeParser:
    map = {
        'i': ['trade_id', str],
        'T': ['trade_timestamp_utc', Parser.parse_datetime],
        'p': ['trade_price', float],
        'v': ['trade_size', float],
        'S': ['direction', str],
        's': ['symbol', str],
        'BT': ['is_block_trade', bool],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in TradeParser.map.values()]

        ls = []
        for d in msg['data']:

            dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in d.items()
                   if (key_value_pair := TradeParser.map.get(k, k))[0] in subset}

            ls.append(dct)

        dct = {'respond_time_utc': Parser.parse_datetime(msg['ts']), 'trade': ls}

        return dct


if __name__ == "__main__":

    # cls = BybitWS(verbose=10, markets=['ETHUSDT'], channels=['tickers'], redis_key='foo')
    # cls = BybitWS(verbose=10, markets=['ETHUSDT'], channels=['publicTrade'], redis_key='foo')
    # cls = BybitWS(verbose=10, markets=['ETHUSDT'], channels=['orderbook.1'], redis_key='foo')
    cls = BybitWS(verbose=10, markets=['ETHUSDT'], channels=['kline.15'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['btcusdt'], channels=['trade.detail'], redis_key='foo'
    # cls = HuobiWS(verbose=10, markets=['btcusdt'], channels=['detail'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['ethbtc'], channels=['depth.step1'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['ethbtc'], channels=['kline.1min'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['btcusdt'], channels=['bbo'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['ethbtc'], channels=['mbp.refresh.5'], redis_key='foo')

    cls.run()
