import datetime as dt

from crypto_ws.core_ws import CoreWS


class BinanceWS(CoreWS):

    CACHING_KEY = 'default_redis_caching_key:binance'
    PUBLISH_CHANNEL = 'default_redis_publish_key:binance'

    CHANNELS = ['trade', 'ticker', 'index', 'kline_1m', 'kline_5m']

    def __init__(self, url='wss://stream.binance.com:9443/ws', markets=('btcusdt', 'ethusdt'), channels=('trade',),
                 caching_freq=0.25, translate=None, redis_kwargs=None, **kwargs):

        super().__init__(url, markets, channels, caching_freq, translate, redis_kwargs, **kwargs)

    # ----

    def _keep_alive(self):
        self._socket.ping('keepalive')

    # ----

    def _subscribe(self):

        subscriptions = [[f"{m}@{c}" for m in self.markets] for c in self.channels]

        for idx, subscription in enumerate(subscriptions):
            self._send({'method': 'SUBSCRIBE', 'params': subscription, 'id': idx})

    def _loop(self):

        while True:

            self._heart_beat()
            data = self._rcv()

            if isinstance(data, dict) and 'e' in data.keys():

                channel = data['e']
                market = self._do_translate(data['s'].lower())

                if channel == '24hrTicker':
                    channel = 'ticker'
                    msg = TickerParser.parse(data, subset=None)

                elif channel == 'trade':
                    msg = TradeParser.parse(data, subset=None)

                elif channel == 'index':
                    msg = TradeParser.parse(data, subset=None)

                elif 'kline' in channel:
                    channel = f"kline_{data['k']['i']}"
                    msg = BarParser.parse(data, subset=None)
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


class TradeParser:

    map = {
        'e': ['event_type', str],
        'E': ['event_time_utc', Parser.parse_datetime],
        's': ['symbol', str],
        't': ['trade_id', int],
        'p': ['price', float],
        'q': ['quantity', float],
        'b': ['buy_order_id', int],
        'a': ['sell_order_id', int],
        'T': ['trade_time', int],
        'S': ['side', int]
    }

    @staticmethod
    def parse(msg, subset=None):

        subset = subset if subset else ['event_type', 'event_time_utc', 'symbol', 'price', 'quantity', 'side',
                                        'trade_id']

        return {key_value_pair[0]: key_value_pair[1](v) for k, v in msg.items()
                if (key_value_pair := TradeParser.map.get(k, k))[0] in subset}


class TickerParser:

    map = {
        'e': ['event_type', str],
        'E': ['event_time_utc', Parser.parse_datetime],
        's': ['symbol', str],
        'o': ['open', float],
        'h': ['high', float],
        'l': ['low', float],
        'c': ['close', float],
        'V': ['traded_volume_contract', float],
        'A': ['traded_volume_asset', float],
        'P': ['price_change_pct', float],
        'Q': ['price_change', float],
        'F': ['first_trade_id', int],
        'L': ['last_trade_id', int],
        'n': ['number_trades', int],
        'bo': ['bid', float],
        'ao': ['ask', float],
        'bq': ['bid_quantity', float],
        'aq': ['ask_quantity', float],
        'b': ['buy_implied_volatility', float],
        'a': ['sell_implied_volatility', float],
        'd': ['delta', float],
        't': ['theta', float],
        'g': ['gamma', float],
        'v': ['vega', float],
        'vo': ['implied_volatility', float],
        'mp': ['mark_price', float],
        'hl': ['maximum_buy_price', float],
        'll': ['minimum_sell_price', float],
        'eep': ['estimated_strike_price', float]
    }

    @staticmethod
    def parse(msg, subset=None):

        subset = subset if subset else ['event_type', 'event_time_utc', 'open', 'high', 'low', 'close',
                                        'price_change_pct', 'traded_volume_asset', 'bid', 'ask', 'bid_quantity',
                                        'ask_quantity', 'symbol']

        return {key_value_pair[0]: key_value_pair[1](v) for k, v in msg.items()
                if (key_value_pair := TickerParser.map.get(k, k))[0] in subset}


class IndexParser:

    map = {
        'e': ['event_type', str],
        'E': ['event_time_utc', Parser.parse_datetime],
        's': ['symbol', str],
        'p': ['price', float]
    }

    @staticmethod
    def parse(msg, subset=None):

        subset = subset if subset else ['event_type', 'event_time_utc', 'symbol', 'price', 'quantity', 'side',
                                        'trade_id']

        return {key_value_pair[0]: key_value_pair[1](v) for k, v in msg.items()
                if (key_value_pair := IndexParser.map.get(k, k))[0] in subset}


class BarParser:

    map = {
        'e': ['event_type', str],
        'E': ['event_time_utc', Parser.parse_datetime],
        's': ['symbol', str],
        't': ['start_time_utc', Parser.parse_datetime],
        'T': ['end_time_utc', Parser.parse_datetime],
        'i': ['period', str],
        'F': ['first_trade_id', int],
        'L': ['last_trade_id', int],
        'o': ['open', float],
        'c': ['close', float],
        'h': ['high', float],
        'l': ['low', float],
        'v': ['traded_volume_contract', float],
        'n': ['number_trades', int],
        'x': ['current_candle_completed', bool],
        'q': ['traded_volume_asset', float],
        'V': ['traded_volume_contract_taker', float],
        'Q': ['number_trades_taker', int]
    }

    @staticmethod
    def parse(msg, subset=None):

        subset = subset if subset else ['event_type', 'event_time_utc', 'symbol', 'start_time_utc', 'end_time_utc',
                                        'period', 'open', 'high', 'low', 'close', 'number_trades',
                                        'current_candle_completed']
        msg.update(msg['k'])
        msg.pop('k')

        dct = {key_value_pair[0]: key_value_pair[1](v) for k, v in msg.items()
               if (key_value_pair := BarParser.map.get(k, k))[0] in subset}

        if dct['current_candle_completed']:
            return dct

        return None


if __name__ == "__main__":

    redis_kw = dict(host='localhost', password=None)

    cls = BinanceWS(verbose=10, markets=['btcusdt'], channels=['trade'])

    cls.run()
