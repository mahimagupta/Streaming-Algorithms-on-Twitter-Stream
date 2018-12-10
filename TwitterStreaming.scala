import java.util.logging.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.mutable.Map
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status


object TwitterStreaming {

  var stream_count = 0
  val reservoir_size = 100
  var length_total = 0
  var sample_tweets = new ListBuffer[Status]()
  var hashtagsDict = Map[String, Int]()


  val consumerkey = "YOUR_KEY"
  val consumer_secret = "YOUR_KEY"
  val accesstoken = "YOUR_KEY"
  val accesstoken_secret = "YOUR_KEY"

  def ProcessingTweets(tweetBatches : RDD[Status]) : Unit =
  {

    val tweets = tweetBatches.collect()
    for(status <- tweets)
    {

      if(stream_count < reservoir_size)
      {
        sample_tweets.append(status)
        length_total = length_total + status.getText().length

        val hashTags = status.getHashtagEntities().map(_.getText)
        for(tweet_tag <- hashTags)
        {
          if(hashtagsDict.contains(tweet_tag))
          {
            hashtagsDict(tweet_tag) += 1
          }
          else
          {
            hashtagsDict(tweet_tag) = 1
          }
        }
      }

      else
      {
        val i = Random.nextInt(stream_count)

        if(i < reservoir_size)
        {
          val tweet_to_be_removed = sample_tweets(i)
          sample_tweets(i) = status
          length_total = length_total + status.getText().length - tweet_to_be_removed.getText().length


          val hashTags = tweet_to_be_removed.getHashtagEntities().map(_.getText)
          for(tweet_tag <- hashTags)
          {
            hashtagsDict(tweet_tag) -= 1
          }

          val new_HashTags = status.getHashtagEntities().map(_.getText)
          for(tweet_tag <- new_HashTags)
          {
            if(hashtagsDict.contains(tweet_tag))
            {
              hashtagsDict(tweet_tag) += 1
            }
            else
            {
              hashtagsDict(tweet_tag) = 1
            }
          }

          val top_Tags = hashtagsDict.toSeq.sortWith(_._2 > _._2)
          val length = top_Tags.size.min(5)

          println("The number of the twitter from beginning: " + (stream_count + 1))

          println("Top 5 hot HashTags:")

          for(j <- 0 until length)
          {
            if(top_Tags(j)._2 != 0)
            {
              println(top_Tags(j)._1 + ":" + top_Tags(j)._2)
            }
          }

          println("The average length of the twitter is: " +  length_total/(reservoir_size.toFloat))

          println("\n\n")

        }
      }

      stream_count = stream_count + 1

    }
  }

  def main(args: Array[String]): Unit =
  {

    val spcf = new SparkConf().setAppName("Mahima_Gupta_TwitterStreaming").setMaster("local[*]")
    val sctxt = new SparkContext(spcf)

    sctxt.setLogLevel(logLevel = "OFF")

    System.setProperty("twitter4j.oauth.consumerKey", consumerkey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumer_secret)
    System.setProperty("twitter4j.oauth.accessToken", accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accesstoken_secret)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spstc = new StreamingContext(sctxt, Seconds(10))
    val finaltweets = TwitterUtils.createStream(spstc, None, Array("Data"))
    finaltweets.foreachRDD(tweetBatches => ProcessingTweets(tweetBatches))

    spstc.start()
    spstc.awaitTermination()
  }
}
