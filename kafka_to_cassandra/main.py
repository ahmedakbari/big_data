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
                    id bigint,
                    primary key((month, day), hour)
                    ) WITH CLUSTERING ORDER BY (hour DESC)
                     """)

# Second Table
session.execute("""
                    CREATE TABLE IF not Exists Hashtag(
                       month text,
                       day int,
                       hour int,
                       hashtag text,
                       id bigint,
                       primary key(hashtag, month, day, hour)
                    ) WITH CLUSTERING ORDER BY (month DESC, day DESC, hour DESC)
                    """)

# Third Table
session.execute("""
                    CREATE TABLE IF not Exists Keywords(
                       month text,
                       day int,
                       hour int,
                       keywords text,
                       id bigint,
                       primary key(keywords, month, day, hour)
                       ) WITH CLUSTERING ORDER BY (month DESC, day DESC, hour DESC)
                    """)

# Forth Table
session.execute("""
                    CREATE TABLE IF not Exists UserName(
                       month text,
                       day int,
                       hour int,
                       id bigint,
                       primary key(id, month, day, hour)
                       ) WITH CLUSTERING ORDER BY (month DESC, day DESC, hour DESC)
                    """)

# Fifth Table
session.execute("""
                    CREATE TABLE IF not Exists All(
                       month text,
                       day int,
                       hour int,
                       id bigint,
                       constant text,
                       primary key(constant, month, day, hour)
                       )
                    """)
print("created table...")

prepared1 = session.prepare("""
                                INSERT INTO Date (month, day, hour, id)
                                VALUES (?, ?, ?, ?)
                                """)
prepared2 = session.prepare("""
                                INSERT INTO Hashtag (month, day, hour, hashtag, id)
                                VALUES (?, ?, ?, ?, ?)
                                """)
prepared3 = session.prepare("""
                                INSERT INTO Keywords (month, day, hour, keywords, id)
                                VALUES (?, ?, ?, ?, ?)
                                """)
prepared4 = session.prepare("""
                                INSERT INTO UserName (month, day, hour, id)
                                VALUES (?, ?, ?, ?)
                                """)
prepared5 = session.prepare("""
                                INSERT INTO All (month, day, hour, id, constant)
                                VALUES (?, ?, ?, ?, ?)
                                """)

for message in consumer:
    print("inserting row")
    msg = json.loads(message.value)
    ll = len(msg['hashtag'])
    kk = len(msg['keyword'])

    session.execute(prepared1, (msg['month'], msg['day'], msg['hour'], msg['id']))
    if ll >= 1:
        for i in range(1, ll):
            session.execute(prepared2, (msg['month'],msg['day'],msg['hour'],msg['hashtag'][i],msg['id']))
    if kk >= 1:
        for j in range(1, kk):
            session.execute(prepared3, (msg['month'],msg['day'],msg['hour'],msg['keyword'][j],msg['id']))
    session.execute(prepared4, (msg['month'],msg['day'],msg['hour'],msg['id']))
    session.execute(prepared5, (msg['month'],msg['day'],msg['hour'],msg['id'],msg['constant']))