from crypto_ws.binance_ws import BinanceWS
import redis
import json


def get_cached_data():
    r = redis.Redis(host='localhost', password=None)
    foo = r.get(name='redis_cache_key:ticker').decode()

    return json.loads(foo)


if __name__ == '__main__':

    redis_kw = dict(host='localhost', password=None)

    cls = BinanceWS(verbose=10, caching_key='redis_cache_key', publish_channel='redis_pub_channel',
                    markets=['btcusdt', 'ethusdt'], channels=['ticker'],

                    caching_freq=10,
                    do_cache=True, do_publish=False, redis_kwargs=redis_kw)

    cls.run()
