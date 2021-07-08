import numpy as np
import pandas as pd
import re
import nltk

import json

from sklearn.feature_extraction.text import TfidfVectorizer

import numpy


def largest_indices(ary, n):
    """Returns the n largest indices from a numpy array."""
    flat = ary.flatten()
    indices = np.argpartition(flat, -n)[-n:]
    indices = indices[np.argsort(-flat[indices])]
    return np.unravel_index(indices, ary.shape)


class keyword_extraction():

    def __init__(self) -> None:
        super().__init__()
        with open("persian", "r", encoding="utf-8")as f:
            stopwords = f.read().split("\n")
        self.tfidfconverter = TfidfVectorizer(max_features=2000, min_df=2, max_df=0.7, stop_words=stopwords)
        self.processed_tweets = []

    def clean_text(self, text):
        # Remove all the special characters
        processed_tweet = re.sub(r'\W', ' ', str(text))

        # remove all single characters
        processed_tweet = re.sub(r'\s+[a-zA-Z]\s+', ' ', processed_tweet)

        # Substituting multiple spaces with single space
        processed_tweet = re.sub(r'\s+', ' ', processed_tweet, flags=re.I)

        # Removing prefixed 'b'
        processed_tweet = re.sub(r'^b\s+', '', processed_tweet)
        return processed_tweet

    def find_words(self, tf_idf):
        a = largest_indices(tf_idf[-3], 3)
        cc = a[0]
        words = []
        for vocab in self.tfidfconverter.vocabulary_:
            index = int(self.tfidfconverter.vocabulary_[vocab])
            if index in list(cc):
                print(vocab)
                words.append(vocab)
        return words

    def extract(self, text):
        clean_text = self.clean_text(text)
        self.processed_tweets.append(clean_text)
        if (len(self.processed_tweets) > 2000):
            del self.processed_tweets[0]
        if(len(self.processed_tweets)<20):
            return []
        tf_idf = self.tfidfconverter.fit_transform(self.processed_tweets).toarray()
        keywords = self.find_words(tf_idf)
        return keywords
