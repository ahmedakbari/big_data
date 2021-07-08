import re
from kafka import KafkaConsumer
import numpy
import json
from json import JSONEncoder
from kafka import KafkaProducer

from keyword_extractor import keyword_extraction


class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)


producer = KafkaProducer(bootstrap_servers='kafka:9092')
consumer = KafkaConsumer('preprocess', bootstrap_servers='kafka:9092', group_id='per_process_consumer')
keyword_extractor = keyword_extraction()
for msg in consumer:
    tweet = json.loads(msg.value)
    print(tweet['id'])
    text = tweet['full_text']
    tweet['hashtag'] = re.findall(r"#(\w+)", text)
    tweet['url'] = re.findall(r'http\S+', text)
    tweet['mention'] = re.findall(r'@(\S+)', text)
    tweet['keyword'] = keyword_extractor.extract(tweet)
    tweet = json.dumps(tweet, cls=NumpyArrayEncoder).encode('utf-8')
    future = producer.send('persistence', tweet)
    result = future.get(timeout=60)
    print("tweet has been processed")
