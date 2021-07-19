import re
from kafka import KafkaConsumer
import numpy
import json
from json import JSONEncoder
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import warnings

warnings.filterwarnings('ignore')

elasticSearch = Elasticsearch([{'host':'elasticsearch', 'port': 9200}])


class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

consumer = KafkaConsumer('persistence', bootstrap_servers='kafka:9092', group_id='persistence_consumer')

for message in consumer:
    msg = json.loads(message.value)

    try:
        elasticSearch.index(index='persistence', doc_type='persistence', body=msg)
    except Exception as e:
        print(e)
