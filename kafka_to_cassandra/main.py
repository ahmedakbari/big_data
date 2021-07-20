import re
import numpy
import json

from json import JSONEncoder
from kafka import KafkaConsumer

from cassandra.cluster import Cluster

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

consumer = KafkaConsumer('persistence', bootstrap_servers ='kafka:9092', group_id = 'cassandra_consumer')

cluster = Cluster(['cassandra'], port=9042)
session = cluster.connect()
session.execute('''create keyspace IF NOT EXISTS cas with replication={'class': 'SimpleStrategy', 'replication_factor' : 3};''')

session = cluster.connect('cas', wait_for_all_pools=True)

print("creating table...")

# First Table
session.execute("""
                    CREATE TABLE IF not Exists Date(
                    month text,
                    day int,
                    hour int,
                    minute int,
                    id bigint,
                    user_name text,
                    primary key((month, day), hour, minute, user_name)
                    ) WITH CLUSTERING ORDER BY (hour DESC)
                     """)

# Second Table
session.execute("""
                    CREATE TABLE IF not Exists Hashtag(
                       month text,
                       day int,
                       hour int,
                       minute int,
                       hashtag text,
                       id bigint,
                       user_name text,
                       primary key(hashtag, month, day, hour, minute, user_name)
                    ) WITH CLUSTERING ORDER BY (month DESC, day DESC, hour DESC)
                    """)

# Third Table
session.execute("""
                    CREATE TABLE IF not Exists Keywords(
                       month text,
                       day int,
                       hour int,
                       minute int,
                       keywords text,
                       id bigint,
                       user_name text,
                       primary key(keywords, month, day, hour, minute, user_name)
                       ) WITH CLUSTERING ORDER BY (month DESC, day DESC, hour DESC)
                    """)

# Forth Table
session.execute("""
                    CREATE TABLE IF not Exists UserName(
                       month text,
                       day int,
                       hour int,
                       minute int,
                       id bigint,
                       user_name text,
                       primary key(user_name, month, day, hour, minute)
                       ) WITH CLUSTERING ORDER BY (month DESC, day DESC, hour DESC)
                    """)

# Fifth Table
session.execute("""
                    CREATE TABLE IF not Exists All(
                       month text,
                       day int,
                       hour int,
                       minute int,
                       id bigint,
                       constant text,
                       user_name text,
                       primary key(constant, month, day, hour, minute, user_name)
                       )
                    """)
print("created table...")

prepared1 = session.prepare("""
                                INSERT INTO Date (month, day, hour, minute, id, user_name)
                                VALUES (?, ?, ?, ?, ?, ?)
                                """)
prepared2 = session.prepare("""
                                INSERT INTO Hashtag (month, day, hour, minute, hashtag, id, user_name)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """)
prepared3 = session.prepare("""
                                INSERT INTO Keywords (month, day, hour, minute, keywords, id, user_name)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """)
prepared4 = session.prepare("""
                                INSERT INTO UserName (month, day, hour, minute, id, user_name)
                                VALUES (?, ?, ?, ?, ?, ?)
                                """)
prepared5 = session.prepare("""
                                INSERT INTO All (month, day, hour, minute, id, constant, user_name)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """)

for message in consumer:
    print("inserting row")
    msg = json.loads(message.value)
    ll = len(msg['hashtag'])
    kk = len(msg['keyword'])

    session.execute(prepared1, (msg['month'], msg['day'], msg['hour'], msg['minute'], msg['id'], msg['user']['screen_name']))
    if ll >= 1:
        for i in range(1, ll):
            session.execute(prepared2, (msg['month'], msg['day'], msg['hour'], msg['minute'], msg['hashtag'][i],msg['id'], msg['user']['screen_name']))
    if kk >= 1:
        for j in range(1, kk):
            session.execute(prepared3, (msg['month'], msg['day'], msg['hour'], msg['minute'], msg['keyword'][j], msg['id'], msg['user']['screen_name']))
    session.execute(prepared4, (msg['month'], msg['day'], msg['hour'], msg['minute'], msg['id'], msg['user']['screen_name']))
    session.execute(prepared5, (msg['month'], msg['day'], msg['hour'], msg['minute'], msg['id'], msg['constant'], msg['user']['screen_name']))

