import time
import json
import uuid
import tweepy
# assuming twitter_authentication.py contains each of the 4 oauth elements (1 per line)
API_KEY="2QX1YKQKheOsezZgotXZoiBXc"
API_SECRET='XWDOXGljhAP03SU1xweQS6PmoegvPBnHHdFwAadG6CnPTnWHjK'
ACCESS_TOKEN='15257539-ERDMc7Ezn7t0tLmfBRRruyYpGmIsN43hsGSHdQS64'
ACCESS_TOKEN_SECRET='1DH7FHDcqgHX3YxW2ZcvU91dkaZcogISXUevCw1PxScoQ'

class twitter_crowler():

    def __init__(self) -> None:
        super().__init__()
        self.auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
        self.auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        self.api = tweepy.API(self.auth, parser=tweepy.parsers.JSONParser())

        self.tweets_for_10k=[]
        self.since_id_map={}
        self.queries=['کن','کرد','باش','بود','شو','شد','دار','داشت','خواه','خواست','گوی','گفت','گیر','گرفت','آی','آمد','توان','تونست','یاب','آور','در','به','از','كه','می','این','است','را','با','امروز','ایم','دادیم','یک','دو','یه','چهار','چار','پنج','شش','هفت','هشت','نه','ده','صد','هزار','دویست','ما','تو','پول','بورس']
        for q in self.queries:
            self.since_id_map[q]=0
        self.max_tweets = 10
        self.since_id=0
        self.since_count = 10000
    def crowl(self):
        while True:
            for query in self.queries:
                try:
                    results = self.api.search(q=query, count=10, tweet_mode='extended',lang='fa',since_id=self.since_id_map[query])
                    search_metadata=results['search_metadata']
                    self.since_id_map[query]=search_metadata['max_id_str']
                    for tweet in results['statuses']:
                        try:
                            tweet['full_text']=tweet['retweeted_status']['full_text']
                        except:
                            pass
                        # print(len(tweets_for_10k),tweet['full_text'].replace("\n"," "))
                        yield tweet
                except:
                    print("sleep 60 seconds")
                    time.sleep(60)
                time.sleep(0.5)
        # json.dump(tweets_for_10k, open("file.json", "w", encoding="utf-8"))