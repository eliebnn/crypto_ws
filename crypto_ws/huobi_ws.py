import datetime as dt
import gzip
import json

from crypto_ws.core_ws import CoreWS


class HuobiWS(CoreWS):

    CACHING_KEY = 'default_redis_caching_key:huobi'
    PUBLISH_CHANNEL = 'default_redis_publish_key:huobi'

    CHANNELS = ['ticker', 'bbo', 'trade.detail', 'detail',
                'depth.step0', 'depth.step1', 'depth.step2', 'depth.step3', 'depth.step4', 'depth.step5',
                'mbp.refresh.5', 'mbp.refresh.10', 'mbp.refresh.20',
                'kline.1min', 'kline.5min', 'kline.15min', 'kline.30min']

    def __init__(self, url='wss://api.huobi.pro/ws', markets=('btcusdt', 'ethusdt'), channels=('ticker',),
                 caching_freq=0.25, translate=None, redis_kwargs=None, **kwargs):

        super().__init__(url, markets, channels, caching_freq, translate, redis_kwargs, **kwargs)

    # ----

    def _heart_beat(self, msg):
        self._send({'pong': msg['ping']}) if 'ping' in msg.keys() else None

    # ----

    def _subscribe(self):

        for m in self.markets:
            for c in self.channels:
                self._send({"sub": f"market.{m}.{c}"})

    def _rcv(self):
        msg = self._socket.recv()
        msg = gzip.decompress(msg).decode()
        msg = msg if msg != '' else '{}'

        return json.loads(msg)

    def _loop(self):
        while True:

            data = self._rcv()
            self._heart_beat(data)

            if isinstance(data, dict) and 'tick' in data.keys():

                key = data['ch']
                _, market, channel = key.split('.', 2)
                market = self._do_translate(market)

                if 'ticker' in channel:
                    msg = TickerParser.parse(data, subset=None)

                elif 'kline.' in channel:
                    msg = BarParser.parse(data, subset=None)

                elif 'depth.step' in channel:
                    msg = DepthParser.parse(data, subset=None)

                elif 'mbp.refresh' in channel:
                    msg = ByPriceParser.parse(data, subset=None)

                elif 'bbo' in channel:
                    msg = BBOParser.parse(data, subset=None)

                elif 'trade.detail' in channel:
                    msg = TradeParser.parse(data, subset=None)

                elif 'detail' in channel:
                    msg = DetailParser.parse(data, subset=None)

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
        'id': ['id', str],
        'amount': ['amount', float],
        'count': ['count', int],
        'open': ['open', float],
        'close': ['close', float],
        'low': ['low', float],
        'high': ['high', float],
        'vol': ['vol', float],
        'ask': ['ask', float],
        'askSize': ['askSize', float],
        'bid': ['bid', float],
        'bidSize': ['bidSize', float],
        'lastPrice': ['lastPrice', float],
        'lastSize': ['lastSize', float]
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in TickerParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['tick'].items()
               if (key_value_pair := TickerParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


class BarParser:
    map = {
        'id': ['id', str],
        'amount': ['amount', float],
        'count': ['count', int],
        'open': ['open', float],
        'close': ['close', float],
        'low': ['low', float],
        'high': ['high', float],
        'vol': ['vol', float]
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in BarParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['tick'].items()
               if (key_value_pair := BarParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


class DepthParser:
    map = {
        'ts': ['time_utc', Parser.parse_datetime],
        'version': ['version', str],
        'bids': ['bids', list],
        'asks': ['asks', list]
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in DepthParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['tick'].items()
               if (key_value_pair := DepthParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


class ByPriceParser:

    map = {
        'seqNum': ['seqNum', str],
        'bids': ['bids', list],
        'asks': ['asks', list]
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in ByPriceParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['tick'].items()
               if (key_value_pair := ByPriceParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


class BBOParser:

    map = {
        'seqId': ['seqId', str],
        'ask': ['ask', float],
        'askSize': ['askSize', float],
        'bid': ['bid', float],
        'bidSize': ['bidSize', float],
        'quoteTime': ['quote_time_utc', Parser.parse_datetime],
        'symbol': ['symbol', str],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in BBOParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['tick'].items()
               if (key_value_pair := BBOParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


class TradeParser:

    map = {
        'id': ['id', str],
        'ts': ['trade_timestamp_utc', Parser.parse_datetime],
        'tradeId': ['tradeId', str],
        'amount': ['amount', float],
        'price': ['price', float],
        'direction': ['direction', str],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in TradeParser.map.values()]

        ls = []
        for d in msg['tick']['data']:
            dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in d.items()
                   if (key_value_pair := TradeParser.map.get(k, k))[0] in subset}

            ls.append(dct)

        dct = {'respond_time_utc': Parser.parse_datetime(msg['ts']), 'trade': ls,
               'last_creation_utc': Parser.parse_datetime(msg['tick']['ts'])}

        return dct


class DetailParser:

    map = {
        'id': ['id', str],
        'amount': ['amount', float],
        'count': ['count', int],
        'open': ['open', float],
        'close': ['close', float],
        'low': ['low', float],
        'high': ['high', float],
        'vol': ['vol', float],
        'version': ['version', str],
    }

    @staticmethod
    def parse(msg, subset=None):
        subset = subset if subset else [k[0] for k in DetailParser.map.values()]

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg['tick'].items()
               if (key_value_pair := DetailParser.map.get(k, k))[0] in subset}

        dct['respond_time_utc'] = Parser.parse_datetime(msg['ts'])

        return dct


if __name__ == "__main__":

    # cls = HuobiWS(verbose=10, markets=['ethbtc'], channels=['ticker'], redis_key='foo')
    cls = HuobiWS(verbose=10, markets=['btcusdt'], channels=['trade.detail'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['btcusdt'], channels=['detail'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['ethbtc'], channels=['depth.step1'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['ethbtc'], channels=['kline.1min'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['btcusdt'], channels=['bbo'], redis_key='foo')
    # cls = HuobiWS(verbose=10, markets=['ethbtc'], channels=['mbp.refresh.5'], redis_key='foo')

    cls.run()
