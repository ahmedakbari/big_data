import re
from kafka import KafkaConsumer
import numpy
import json
from json import JSONEncoder
from kafka import KafkaProducer

#####################################################3
import nltk
from hazm import Normalizer
from hazm import sent_tokenize, word_tokenize
from hazm import Stemmer, Lemmatizer
from hazm import POSTagger
from hazm import DependencyParser
from hazm import stopwords_list
from nltk.stem.porter import PorterStemmer
#####################################################3

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

    ############################################
    date = tweet['created_at']
    tweet['day'] = int(date[8:10])
    tweet['hour'] = int(date[11:13])
    tweet['month'] = date[4:7]
    tweet['constant'] = '1'
    Text = text

    normalizer = Normalizer()
    lemmatizer = Lemmatizer()
    stemmer = PorterStemmer()

    StopWord = stopwords_list()+['ها']

    Text = normalizer.normalize(Text)
    sms = re.sub('[^آ-ی]', ' ', Text)
    tokenized_sms = word_tokenize(sms)
    for word in tokenized_sms:
        if word in StopWord:
            tokenized_sms.remove(word)
    for j in range(len(tokenized_sms)):
        tokenized_sms[j] = stemmer.stem(tokenized_sms[j])
    Text = " ".join(tokenized_sms)
    tweet['word'] = Text.split()

    ############################################

    tweet = json.dumps(tweet, cls=NumpyArrayEncoder).encode('utf-8')
    future = producer.send('persistence', tweet)
    result = future.get(timeout=60)
    print("tweet has been processed")
