import sys
import time

import json
from json import JSONEncoder
import traceback
import numpy

from twitter_crowler import twitter_crowler
from kafka import KafkaProducer

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)


producer = KafkaProducer(bootstrap_servers='kafka:9092')

t_crowler = twitter_crowler()

running = True
backup_tweets = json.load(open("file.json", "r", encoding="utf-8"))
for tweet in backup_tweets:
    # try:
    tweet = json.dumps(tweet, cls=NumpyArrayEncoder).encode('utf-8')
    future = producer.send('preprocess',tweet)
    result = future.get(timeout=60)

for message in t_crowler.crowl():
    print(message)
    tweet = json.dumps(message, cls=NumpyArrayEncoder).encode('utf-8')
    future = producer.send('preprocess', tweet)
    result = future.get(timeout=60)
    time.sleep(1)
