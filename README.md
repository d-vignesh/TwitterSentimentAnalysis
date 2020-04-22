TwitterStreamAnalysis is an application written on Pyspark that tracks the occurence of each hashtag and also does a sentiment analysis on the tweets associated with each hashtag thus providing us top trending hashtags with positive and negative tweets on each hashtag.Also the application works on Streaming api in order to get a real time update.

In TweetsAnalyzer.ipynb we build the classifier model using Naive Bayes Classifier. The model is trained on Sentiment140 dataset.
 
In TweetsConsumer.py we consumes the tweets using the Twitter tweepy api and push the tweets to a kafka queue.

In TweetsFetcher.py we build the streaming logic that continously polls the kafka queue for new tweets, performs some text processing and extracts the avialable hashtags and performs a sentiment analysis on the tweet using the model we build in TweetsAnalyzer.

