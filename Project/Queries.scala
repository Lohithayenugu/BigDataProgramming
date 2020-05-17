import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Queries {

  def main(args: Array[String]): Unit = {

    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Queries")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Queries")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // We are using all 3 Fifa dataset given on Kaggle Repository
    //a.Import the dataset and create df and print Schema

    val df1 = spark.read
      .format("json")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("dataset/COVID19_Data1.json")

    // Printing the Schema
    df1.printSchema()

    //b.Perform   10   intuitive   questions   in   Dataset
    //For this problem we have used the Spark SqL on DataFrames

    //First of all create three Temp View
    df1.createOrReplaceTempView("Covid")


    // Find the winner by years using WorldCup view
    val Q1 = spark.sql("select count(1) retweet_count from Covid WHERE isretweet LIKE '%True%' ")
    Q1.show()

    var Q2=spark.sql("select tweetuser user, count(1) tweets from Covid GROUP BY tweetuser ORDER BY tweets DESC LIMIT 5")
    Q2.show();

    //Teams with the most world cup final victories on WorldCup view
    val Q3 = spark.sql("select tweetuser user,count(1) quoted_tweets,isquote from Covid where isquote LIKE '%True%' " +
      "GROUP BY tweetuser,isquote ORDER BY quoted_tweets DESC LIMIT 5")
    Q3.show()

    // Find the winner by years using WorldCup view
    val Q4 = spark.sql("select count(1) tweets_USA from Covid WHERE lower(tweettext) LIKE '%usa%' OR" +
      " lower(tweettext) LIKE '%united states%' OR lower(tweettext) LIKE '%america%' ")
    Q4.show()

  }

}