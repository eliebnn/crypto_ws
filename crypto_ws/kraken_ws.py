import datetime as dt
import gzip
import json

from crypto_ws.core_ws import CoreWS


class KrakenWS(CoreWS):

    CACHING_KEY = 'default_redis_caching_key:huobi'
    PUBLISH_CHANNEL = 'default_redis_publish_key:huobi'

    CHANNELS = ['ticker', 'trade', 'spread', 'book-10', 'ohlc-1']

    def __init__(self, url='wss://ws.kraken.com', markets=('btcusdt', 'ethusdt'), channels=('ticker',),
                 caching_freq=0.25, translate=None, redis_kwargs=None, **kwargs):

        super().__init__(url, markets, channels, caching_freq, translate, redis_kwargs, **kwargs)

    # ----

    def _keep_alive(self):
        self._socket.send(json.dumps({'event': 'ping'}))

    # ----

    def _subscribe(self):

        for m in self.markets:
            for c in self.channels:

                payload = {
                    "event": "subscribe",
                    "pair": [m],
                    "subscription": {"name": c}
                }

                self._send(payload)

    def _loop(self):
        while True:

            data = self._rcv()
            self._heart_beat()

            if isinstance(data, list) and data[2] in self.CHANNELS:

                channel = data[2]
                market = self._do_translate(data[3])

                if 'ticker' in channel:
                    msg = TickerParser.parse(data, subset=None)

                elif 'ohlc' in channel:
                    msg = BarParser.parse(data, subset=None)

                elif 'book' in channel:
                    msg = BookParser.parse(data, subset=None)

                elif 'spread' in channel:
                    msg = SpreadParser.parse(data, subset=None)

                elif 'trade' in channel:
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
        return dt.datetime.utcfromtimestamp(float(x)).strftime('%Y-%m-%d %H:%M:%S.%MS')

    @staticmethod
    def flatten(msg):
        return {f'{k}{idx}': i for k, v in msg.items() for idx, i in enumerate(v)}


class TickerParser:

    map = {
        'a0': ['ask_price', float],
        'a1': ['ask_whole_lot_volume', int],
        'a2': ['ask_lot_volume', float],
        'b0': ['bid_price', float],
        'b1': ['bid_whole_lot_volume', int],
        'b2': ['bid_lot_volume', float],
        'c0': ['close_price', float],
        'c1': ['close_lot_volume', float],
        'v0': ['volume_day', float],
        'v1': ['volume_24h', float],
        'p0': ['vwap_day', float],
        'p1': ['vwap_24h', float],
        't0': ['trades_nb_day', int],
        't1': ['trades_nb_24h', int],
        'l0': ['low_day', float],
        'l1': ['low_24h', float],
        'h0': ['high_day', float],
        'h1': ['high_24h', float],
        'o0': ['open_day', float],
        'o1': ['open_24h', float],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in TickerParser.map.values()]

        return {
            key_value_pair[0]: key_value_pair[1](v)
            for k, v in Parser.flatten(msg[1]).items()
            if (key_value_pair := TickerParser.map.get(k, k))[0] in subset
        }


class BarParser:
    map = {
        0: ['start_time_utc', Parser.parse_datetime],
        1: ['end_time_utc', Parser.parse_datetime],
        2: ['open', float],
        3: ['high', float],
        4: ['low', float],
        5: ['close', float],
        6: ['vwap', float],
        7: ['volume', float],
        8: ['count', int]
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in BarParser.map.values()]

        return {
            key_value_pair[0]: key_value_pair[1](k)
            for idx, k in enumerate(msg[1])
            if (key_value_pair := BarParser.map.get(idx, idx))[0] in subset
        }


class BookParser:
    map = {
        0: ['price', float],
        1: ['volume', float],
        2: ['time_utc', Parser.parse_datetime]
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in BookParser.map.values()]

        asks = [{key_value_pair[0]: key_value_pair[1](k) for idx, k in enumerate(_)
                if (key_value_pair := BookParser.map.get(idx, idx))[0] in subset} for _ in msg[1]['as']]

        bids = [{key_value_pair[0]: key_value_pair[1](k) for idx, k in enumerate(_)
                if (key_value_pair := BookParser.map.get(idx, idx))[0] in subset} for _ in msg[1]['bs']]

        return {'bids': bids, 'asks': asks}


class SpreadParser:

    map = {
        0: ['bid', float],
        1: ['ask', float],
        2: ['time_utc', Parser.parse_datetime],
        3: ['bid_volume', float],
        4: ['ask_volume', float],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in SpreadParser.map.values()]

        return {key_value_pair[0]: key_value_pair[1](k) for idx, k in enumerate(msg[1])
                if (key_value_pair := SpreadParser.map.get(idx, idx))[0] in subset}


class TradeParser:

    map = {
        0: ['price', float],
        1: ['volume', float],
        2: ['time_utc', Parser.parse_datetime],
        3: ['side', str],
        4: ['order_type', str],
        5: ['misc', str],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in TradeParser.map.values()]

        ls = []
        for d in msg[1]:
            ls.append({key_value_pair[0]: key_value_pair[1](k) for idx, k in enumerate(d)
                       if (key_value_pair := TradeParser.map.get(idx, idx))[0] in subset})

        return {'trade': ls}


if __name__ == "__main__":

    # cls = KrakenWS(verbose=10, markets=['BTC/USD'], channels=['ticker'])
    # cls = KrakenWS(verbose=10, markets=['BTC/USD'], channels=['book'])
    # cls = KrakenWS(verbose=10, markets=['BTC/USD'], channels=['trade'])
    # cls = KrakenWS(verbose=10, markets=['BTC/USD'], channels=['ohlc'])
    cls = KrakenWS(verbose=10, markets=['BTC/USD'], channels=['spread'])

    cls.run()
