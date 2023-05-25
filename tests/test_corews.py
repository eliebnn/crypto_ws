from unittest.mock import patch

import redis

from crypto_ws.core_ws import CoreWS


def test_init():
    url = 'wss://example.com'
    markets = ['ETH/BTC', 'BTC/USD']
    channels = ['ticker']
    caching_freq = 0.5
    translate = {'ETH/BTC': 'ETH - BTC'}
    redis_kwargs = {'host': 'localhost', 'port': 6379}
    kwargs = {'do_cache': True, 'do_publish': False, 'verbose': 5}

    core_ws = CoreWS(url, markets, channels, caching_freq, translate, redis_kwargs, **kwargs)

    assert core_ws.url == url
    assert core_ws.markets == markets
    assert core_ws.channels == channels
    assert core_ws._redis_timer.limit == caching_freq
    assert core_ws._translate == translate
    assert core_ws._do_cache == kwargs['do_cache']
    assert core_ws._do_publish == kwargs['do_publish']
    assert core_ws.verbose == kwargs['verbose']


def test_do_translate():
    core_ws = CoreWS('wss://example.com', translate={'ETH/BTC': 'ETH - BTC'})
    market = 'ETH/BTC'

    result = core_ws._do_translate(market)

    assert result == 'ETH - BTC'

@patch.object(redis.Redis, 'publish')
def test_publish(mock_redis_publish):
    core_ws = CoreWS('wss://example.com', do_publish=True)
    channel = 'ticker'
    msg = 'Hello, world!'

    core_ws._publish(channel, msg)

    mock_redis_publish.assert_called_with(
        channel=f'{core_ws._publish_channel}:{channel}',
        message=msg
    )
