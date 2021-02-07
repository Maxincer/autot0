import orjson
import redis

REDIS_HOST = '47.103.187.110'
REDIS_PORT = 6379
REDIS_PASS = 'Ms123456'

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)


print(orjson.loads(r.get('market_511990.SH').decode('utf-8')))


