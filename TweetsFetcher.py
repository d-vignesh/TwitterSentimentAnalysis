import json
from kafka import KafkaProducer
import tweepy
import configparser
import time


class TweeterStreamListener(tweepy.StreamListener):
    """this class reads the incoming twitter streams and pushes it to kafka"""

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        self.producer = KafkaProducer(
                            bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x : json.dumps(x).encode('utf-8')
                        )


    def on_data(self, data):
        """
        this method will be called whenever new data arrives from twitter api.
        we will push the obtained stream into kafka queue.
        """
        extracted_data = {}
        data_dict = json.loads(data)
        # print(data_dict.keys())
        if 'extended_tweet' in data_dict:
            print(data_dict['extended_tweet']['full_text'].encode('utf-8'))
            extracted_data['text'] = data_dict['extended_tweet']['full_text']
        elif 'text' in data_dict:
            print(data_dict['text'].encode('utf-8'))
            extracted_data['text'] = data_dict['text']
        
        extracted_data['tags'] = []
        if 'entities' in data_dict and 'hashtags' in data_dict['entities']:
            for tag in data_dict['entities']['hashtags']:
                print(tag['text'])
                extracted_data['tags'].append(tag['text'])
        print('-' * 70)
        if len(extracted_data['tags']) > 0 :
            try:
                self.producer.send('twitterstream', extracted_data)
            except Exception as e:
                print('exception when pushing data to kafka queue')
                print(e)
                return False
        return True

    def on_error(self, status):
        print('error occured when recieving twitter stream')
        return True

    def on_timeout(self):
        print('timeout occured when receiving twitter stream')
        return True


config = configparser.ConfigParser()
config.read('twitter_creds.text')
consumer_key = config['DEFAULT']['consumerKey']
consumer_skey = config['DEFAULT']['consumerSecretKey']
access_key = config['DEFAULT']['accessToken']
access_skey = config['DEFAULT']['accessTokenSecret']

# creating auth obj
auth = tweepy.OAuthHandler(consumer_key, consumer_skey)
auth.set_access_token(access_key, access_skey)
api = tweepy.API(auth)

# creating the stream and binding it to listener
listener = TweeterStreamListener(api)
stream = tweepy.Stream(auth, listener=listener, tweet_mode='extended')
tracks = ['the', 'is', 'i', 'me', 'we', 'was', 'so', 'just', 'on', 'your', 'about'
          'will', 'today', 'got', 'when', 'you', 'u', 'he', 'she', 'his', 'her',
          'up', 'down', 'awesome', 'bad', 'help', 'said', 'ur', 'yours', 'our']
#tracks = ['corona', 'covid', 'india', 'china', 'italy']
stream.filter(track=tracks, languages=['en'])

time.sleep(10)
stream.disconnect()
listener.producer.flush()
listener.producer.close()