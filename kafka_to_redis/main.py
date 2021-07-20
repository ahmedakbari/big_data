import re
import json

from json import JSONEncoder
from kafka import KafkaConsumer
import redis
r = redis.Redis(host="redis", port=6379, db=0)

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

consumer = KafkaConsumer('persistence', bootstrap_servers ='kafka:9092', group_id = 'redis_consumer')

for message in consumer:
    print("inserting row")
    msg = json.loads(message.value)
    ll = len(msg['hashtag'])
    kk = len(msg['keyword'])



    name = msg['user']['screen_name']
    created_at = msg['created_at']
    day = created_at[8:10]
    hour = created_at[11:13]
    for h in msg['hashtag']:
        key = name+ '-' + h + '-' + day + '-' + hour
        r.incrby(key, 1)
        val = r.get(key)
        r.set(key, val, ex=604800)



