from crypto_ws.huobi_ws import HuobiWS
import redis
import json


def get_cached_data():
    r = redis.Redis(host='localhost', password=None)
    foo = r.get(name='redis_cache_key:bbo').decode()

    return json.loads(foo)


if __name__ == '__main__':

    redis_kw = dict(host='localhost', password=None)

    cls = HuobiWS(verbose=10, caching_key='redis_cache_key', publish_channel='redis_pub_channel',
                  markets=['btcusdt', 'ethbtc'], channels=['bbo'], translate={'btcusdt': 'BTC/USDT'},
                  redis_kwargs=redis_kw)

    cls.run()
