import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object KafkaExample {

  trait SENTIMENT_TYPE
  case object VERY_NEGATIVE extends SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object VERY_POSITIVE extends SENTIMENT_TYPE
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE

  def getTwitterStream(ssc: StreamingContext, filters: Seq[String] = Nil, args : Array[String]) = {
    val consumerKey= args(1)
    val consumerSecret= args(2)
    val accessToken= args(3)
    val accessTokenSecret= args(4)

    val conf_builder = new ConfigurationBuilder
    conf_builder.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(conf_builder.build)

    TwitterUtils.createStream(ssc, Some(auth), filters)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("<Topic> <consumer key> <consumer secret> <access token> <access token secret>")
      System.exit(1)
    }
    val topic = args(0)
    val sparkConf = new SparkConf().setAppName("Twitter-Streaming-Sentiment-Analysis")

    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }

    val filters = Seq("happy")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val tweets = getTwitterStream(ssc, filters, args)
    val en_tweets = tweets.filter(_.getLang() == "en")

    en_tweets.foreachRDD(rdd => {
      rdd.cache()
      val property = new Properties()
      val bootstrap = "localhost:9092"
      property.put("bootstrap.servers", bootstrap)
      property.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      property.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](property)
      rdd.collect().toList.foreach(tweet => {
        val txt = tweet.getText()
        val sentiment = get_Sentiment(txt).toString()
        print(txt, sentiment)
        producer.send(new ProducerRecord[String, String](topic, txt, sentiment))
      })
      producer.flush()
      producer.close()
      rdd.unpersist()
    })

    ssc.checkpoint("../checkpoint")
    ssc.start()
    ssc.awaitTermination()

  }
  val standford_prop = {
    val property = new Properties()
    property.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    property
  }
  def get_Sentiment(message: String): SENTIMENT_TYPE = {
    val pipeline = new StanfordCoreNLP(standford_prop)
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length

      println("debug: " + sentiment)
      println("size: " + partText.length)

    }

    val averageSentiment:Double = {
      if(sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if(sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }

    println("debug: main: " + mainSentiment)
    println("debug: avg: " + averageSentiment)
    println("debug: weighted: " + weightedSentiment)

    weightedSentiment match {
      case s if s <= 0.0 => NOT_UNDERSTOOD
      case s if s < 1.5 => NEGATIVE
      case s if s < 3.0 => NEUTRAL
      case s if s < 4.5 => POSITIVE
      case s if s > 4.5 => NOT_UNDERSTOOD

    }

  }

}