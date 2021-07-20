import re
import json,os

from json import JSONEncoder
from kafka import KafkaConsumer
from clickhouse_driver import Client
client = Client(host='clickhouse')

client.execute('CREATE DATABASE IF NOT EXISTS big_data')
client.execute('''
CREATE TABLE IF NOT EXISTS  big_data.tweets
(
`id` UInt32,
`user.id` UInt64,
`user.name` String,
`user.screen_name` String,
`user.followers_count` UInt64,
`user.friends_count` UInt64,
`user.favourites_count` UInt64,
`hashtag` Array(String)
)
Engine=Memory
''')



class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

consumer = KafkaConsumer('persistence', bootstrap_servers ='kafka:9092', group_id = 'clickhouse_consumer')

for message in consumer:
    print("inserting row")
    msg = json.loads(message.value)
    data = {}
    data['id'] = msg["id"]
    data['user.id'] = msg["user"]["id"]
    data['user.screen_name'] = msg["user"]["screen_name"]
    data['user.followers_count'] = msg["user"]["followers_count"]
    data['user.friends_count'] = msg["user"]["friends_count"]
    data['user.favourites_count'] = msg["user"]["favourites_count"]
    data['hashtag'] = msg["hashtag"]


    #print(str(data))
    print(json.dumps(data))
    os.system("echo '"+json.dumps(data)+"' | curl 'http://clickhouse:8123/?query=INSERT%20INTO%20big_data.tweets%20FORMAT%20JSONEachRow' --data-binary @-")
    #client.execute('INSERT INTO big_data.tweets VALUES', json.dumps(data),types_check=True)



