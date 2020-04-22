from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, regexp_replace, lower, explode, window
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
from pyspark.ml.pipeline import PipelineModel
from bs4 import BeautifulSoup

spark = SparkSession.builder \
					.appName('TwitterStream') \
					.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

tweets_schema = StructType([
			StructField('text', StringType()),
			StructField('tags', ArrayType(StringType()))
	])

tweets = spark \
		 .readStream \
		 .format('kafka') \
		 .option('kafka.bootstrap.servers', 'localhost:9092') \
		 .option('subscribe', 'twitterstream') \
		 .option('startingOffsets', 'latest') \
		 .load() \
		 .select(from_json(col('value').cast('string'), tweets_schema).alias('data')) \
		 .select('data.*')

# TEXT PREPROCESSING

# 1. decode the html tags
def decode_html(text):
	""" decodes the html tags present in the tweet"""
	if text:
		return BeautifulSoup(text, 'lxml').get_text()
	else:
		return "NA"

decode_html_udf = udf(decode_html, StringType())

# 2. remove @mention handles 
handles_pat = r'(RT )?@[A-Za-z0-9_]+'

# 3. also we will remove hashtags as we get it as a seperate list
hashtag_pat = r'#[A-Za-z0-9]+'

# 4. remote http url
http_pat = r'https?://[A-Za-z0-9./]+'

# 5. remove www url
www_pat = r'www.[^ ]+'

# 6. decode the text with 'utf-8-sig', to remove utf-8 BOM sequence (\xef\xbf\xbd).
def decode_utf(text):
	try:
		cleaned_text = text.decode('utf-8-sig').replace(u'\ufffd', '?')
	except:
		cleaned_text = text
	return cleaned_text

decode_utf_udf = udf(decode_utf, StringType())

# 7. handle negation words(can't -> can not)

# 8. remove special chars
sp_pat = r'[^A-Za-z]'

# 9. convert to lower case and remove unwanted space added by above steps.
def rem_space(text):
	return ' '.join(text.split())

rem_space_udf = udf(rem_space, StringType())

tweets = tweets \
		 .withColumn('soup_text', decode_html_udf('text')) \
		 .withColumn('han_rem', regexp_replace(col('soup_text'), handles_pat, '')) \
		 .withColumn('tag_rem', regexp_replace(col('han_rem'), hashtag_pat, '')) \
		 .withColumn('http_rem', regexp_replace(col('tag_rem'), http_pat, '')) \
		 .withColumn('www_rem', regexp_replace(col('http_rem'), www_pat, '')) \
		 .withColumn('utf_text', decode_utf_udf('www_rem')) \
		 .withColumn('neg_handel', regexp_replace(col('utf_text'), r"won't", 'will not')) \
		 .withColumn('neg_handel', regexp_replace(col('neg_handel'), r"can't", 'can not')) \
		 .withColumn('neg_handel', regexp_replace(col('neg_handel'), r"n't", ' not')) \
		 .withColumn('sp_rem', regexp_replace(col('neg_handel'), sp_pat, ' ')) \
		 .withColumn('low_text', lower(col('sp_rem'))) \
		 .withColumn('cleaned', rem_space_udf('low_text')) \
		 .selectExpr('cleaned as text', 'tags')

neg_tweets_udf = udf(lambda x : 0.0 if x==1.0 else 1.0, FloatType())

model = PipelineModel.load('./tweets_analyzer.model')
predictions = model.transform(tweets) \
				   .select('tags', 'prediction') \
				   .withColumn('hashtag', explode(col('tags'))) \
				   .withColumn('pos_tweet', col('prediction')) \
				   .withColumn('neg_tweet', neg_tweets_udf('prediction')) \
				   .groupby('hashtag') \
				   .agg(psf.sum('pos_tweet').alias('pos_tweets'), psf.sum('neg_tweet').alias('neg_tweets'), psf.count('pos_tweet').alias('total_tweets'))
				

predictions.writeStream \
		.outputMode('complete') \
		.format('console') \
		.option('truncate', False) \
		.start() \
		.awaitTermination()

