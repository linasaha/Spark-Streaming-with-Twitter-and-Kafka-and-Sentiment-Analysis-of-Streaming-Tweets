# Spark-Streaming-with-Twitter-and-Kafka-and-Sentiment-Analysis-of-Streaming-Tweets
I created a Spark Streaming application that continuously read data from Twitter about a topic. These twitter feeds were analyzed for their sentiment, and then in the next step, Logstash, Elasticsearch, and Kibana were configured to read from the topic and set up visualization of sentiment. To exchange data between Kafka was used as a broker.

I worked with a set of Tweets about US airlines and examined their sentiment polarity.
The dataset was downloaded from the website of Kaggle competition on Twitter US Airline
Sentiment. The aim was to classify the tweets as either “positive”, “neutral”, or “negative” by using two classifiers and pipelines for pre-processing and model building.
