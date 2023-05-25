import json
import redis
from crypto_ws.client_ws import WebsocketClient
from crypto_ws.utils import Timer, obj_to_list


class CoreWS(WebsocketClient):
    """
    A class used to represent a WebSocket client specifically for cryptocurrencies.

    ...

    Attributes
    ----------
    CACHING_KEY : str
        a default string to use as a key when caching data to Redis
    PUBLISH_CHANNEL : str
        a default string to use as a channel when publishing data to Redis

    Methods
    -------
    __init__(url='', markets=('BTC/USD',), channels=('ticker',),
             caching_freq=0.25, translate=None, redis_kwargs=None, **kwargs):
        Constructs all the necessary attributes for the CoreWS object.
    _heart_beat():
        Checks if it's time to send a keep-alive signal.
    _do_translate(market):
        Translates a market name using a provided translation dictionary.
    _cache():
        Caches the results in Redis if the cache time limit has been reached.
    _publish(channel, msg):
        Publishes a message to a channel on Redis.
    """

    CACHING_KEY = 'default_redis_caching_key'
    PUBLISH_CHANNEL = 'default_redis_publish_channel'

    def __init__(self, url='', markets=('BTC/USD',), channels=('ticker',),
                 caching_freq=0.25, translate=None, redis_kwargs=None, **kwargs):
        """
        Constructs all the necessary attributes for the CoreWS object.

        Parameters
        ----------
        url : str
            the URL of the WebSocket server (default is an empty string)
        markets : tuple or list
            the markets to subscribe to (default is ('BTC/USD',))
        channels : tuple or list
            the channels to subscribe to (default is ('ticker',))
        caching_freq : float
            the frequency in seconds to cache results (default is 0.25)
        translate : dict
            a dictionary to use for translating market names (default is None)
        redis_kwargs : dict
            a dictionary of keyword arguments to pass to the Redis constructor (default is None)
        verbose : int
            a int defining the level of verbosity
        caching_key : str
            a string defining a part of the key the data will be cached on in Redis
        publish_channel : str
            a string defining a part of the channel the data will be published on in Redis
        **kwargs : dict
            a dictionary of keyword arguments to control the behaviour of the CoreWS object
        """
        self._redis_timer = Timer(limit=caching_freq)
        self._heart_timer = Timer(limit=50*3, wait=0)

        self._do_cache = kwargs.get('do_cache', False)
        self._do_publish = kwargs.get('do_publish', False)
        self._caching_key = kwargs.get('caching_key', self.CACHING_KEY)
        self._redis = None
        self._init_redis(redis_kwargs)

        self.markets = obj_to_list(markets)
        self.channels = obj_to_list(channels)

        self._publish_channel = kwargs.get('publish_channel', self.PUBLISH_CHANNEL)

        self.results = kwargs.get('results', {c: {} for c in self.channels})
        self._translate = translate if translate else {}
        self.verbose = kwargs.get('verbose', 0)

        super().__init__(url=url)

    def _init_redis(self, redis_kwargs):

        if self._do_cache or self._do_publish:
            kw = redis_kwargs if redis_kwargs else {}
            self._redis = redis.Redis(**kw)

    def _heart_beat(self):
        """
        Checks if it's time to send a keep-alive signal. If it is, sends it and resets the heart timer.
        """
        if self._heart_timer.reached_limit:
            self._keep_alive()
            self._heart_timer.reset_now()

    def _do_translate(self, market):
        """
        Translates a market name using the provided translation dictionary.

        Parameters
        ----------
        market : str
            the market name to translate

        Returns
        -------
        str
            the translated market name
        """
        return self._translate.get(market, market) if self._translate else market

    def _cache(self):
        """
        Caches the results in Redis if the cache time limit has been reached.
        """
        if self._redis_timer.reached_limit and self._do_cache:
            if self.verbose > 6:
                print('Now Caching:')
                print(self.results)

            for channel, result in self.results.items():
                self._redis.set(name=f'{self._caching_key}:{channel}', value=json.dumps(result), ex=60*60)

            self._redis_timer.reset_now()

    def _publish(self, channel, msg):
        """
        Publishes a message to a channel on Redis.

        Parameters
        ----------
        channel : str
            the channel to publish the message to
        msg : str
            the message to publish
        """
        if self._do_publish:
            self._redis.publish(channel=f'{self._publish_channel}:{channel}', message=msg)
