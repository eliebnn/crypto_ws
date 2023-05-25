import json

import redis

from crypto_ws.client_ws import WebsocketClient
from crypto_ws.utils import Timer, obj_to_list


class CoreWS(WebsocketClient):

    CACHING_KEY = 'default_redis_caching_key'
    PUBLISH_CHANNEL = 'default_redis_publish_channel'

    def __init__(self, url='', markets=('BTC/USD',), channels=('ticker',),
                 caching_freq=0.25, translate=None, redis_kwargs=None, **kwargs):

        self._redis_timer = Timer(limit=caching_freq)
        self._heart_timer = Timer(limit=50*3, wait=0)

        self._redis = redis.Redis(**(redis_kwargs if redis_kwargs else {}))
        self._caching_key = kwargs.get('caching_key', self.CACHING_KEY)

        self.markets = obj_to_list(markets)
        self.channels = obj_to_list(channels)

        self._do_cache = kwargs.get('do_cache', True)
        self._do_publish = kwargs.get('do_publish', False)
        self._publish_channel = kwargs.get('publish_channel', self.PUBLISH_CHANNEL)

        self.results = kwargs.get('results', {c: {} for c in self.channels})
        self._translate = translate if translate else {}
        self.verbose = kwargs.get('verbose', 0)

        super().__init__(url=url)

    # ----

    def _heart_beat(self):
        if self._heart_timer.reached_limit:
            self._keep_alive()
            self._heart_timer.reset_now()

    # ----

    def _do_translate(self, market):
        return self._translate.get(market, market) if self._translate else market

    def _cache(self):
        if self._redis_timer.reached_limit and self._do_cache:
            if self.verbose > 6:
                print('Now Caching:')
                print(self.results)

            for channel, result in self.results.items():
                self._redis.set(name=f'{self._caching_key}:{channel}', value=json.dumps(result), ex=60*60)

            self._redis_timer.reset_now()

    def _publish(self, channel, msg):
        if self._do_publish:
            self._redis.publish(channel=f'{self._publish_channel}:{channel}', message=msg)


if __name__ == "__main__":

    cls = CoreWS(verbose=5, caching_key='foo', markets=['ETH/BTC', 'BTC/USD'], translate={'ETH/BTC': 'ETH - BTC'},
                 do_cache=False, do_publish=True)
    cls.run()
