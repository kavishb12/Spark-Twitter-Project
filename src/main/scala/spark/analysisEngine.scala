package spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

class analysisEngine() {
  val spark: SparkSession = SparkSession.builder()
    .appName("Analysis Engine")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * This method takes the provided text and pulls all of the Twitter
   * handles (@...) out and returns them as a List.
   *
   * @param path: string containing path to data file.
   * @return    String with the first 250 characters of all of the handles
   *            in the text.
   */
  def handleStripper(path: String): String = {
    val handlesDf: DataFrame = spark.read.json(path)

    val handlesList = handlesDf
      .select(flattenSchema(handlesDf.schema):_*)
      .select($"text")
      .as[String]
      .flatMap(_.split("\\s"))
      .map(_.replaceAll("[^\\w#@\'\"]+", ""))
      .filter(_.length > 1)
      .filter(_.matches("@\\w*"))
      .map(_.substring(1))
      .groupBy("value")
      .count()
      .sort(functions.desc("count"))
      .collectAsList()
      .toArray

    if(handlesList.isEmpty) {
      val result = "There are no handles or mentions in these Tweets."
      result
    } else {
      val result = new StringBuffer()
      for (handle <- handlesList) {
        if (result.length + handle.toString.length < 250) {
          result.append(handle.toString + "\n")
        }
      }
      result.toString
    }
  }

  /**
   * This method takes the provided text and pulls all of the Twitter
   * hashtags (#...) out and returns them as a List.
   *
   * @param path: string containing path to data file.
   * @return    List[String] with all of the hashtags in the text.
   */
  def hashtagStripper(path: String): String = {
    val hashtagDf: DataFrame = spark.read.json(path)

    val hashtagList = hashtagDf
      .select(flattenSchema(hashtagDf.schema):_*)
      .select($"text")
      .as[String]
      .flatMap(_.split("\\s"))
      .map(_.replaceAll("[^\\w#@\'\"]+", ""))
      .filter(_.length > 1)
      .filter(_.matches("#\\w*"))
      .map(_.substring(1))
      .groupBy("value")
      .count()
      .sort(functions.desc("count"))
      .collectAsList()
      .toArray

    if(hashtagList.isEmpty) {
      val result = "There are no hashtags in these Tweets."
      result
    } else {
      val result = new StringBuffer()
      for (hashtag <- hashtagList) {
        if (result.length + hashtag.toString.length < 250) {
          result.append(hashtag.toString + "\n")
        }
      }
      result.toString
    }

  }

  /**
   * This method takes the provided text and counts all of the words
   * in the text, after filtering through a list of common words,
   * and returns them as a List.
   *
   * @param path: string containing path to data file.
   * @return    A DataFrame with two columns, value = word and count = word count.
   */
  def wordCounter(path: String): String = {
    val wordCount: DataFrame = spark.read.json(path)

    val wordList = wordCount
      .select(flattenSchema(wordCount.schema):_*)
      .select($"text")
      .as[String]
      .flatMap(_.split("\\s"))
      .map(_.replaceAll("[^\\w\"\']+", ""))
      .filter(_.length > 0)
      .map(_.toLowerCase)
      .filter(passesWordFilter _)
      .groupBy("value")
      .count()
      .filter($"count" > 2)
      .sort(functions.desc("count"))
      .collectAsList()
      .toArray

    if(wordList.isEmpty) {
      val result = "There is no text in these Tweets."
      result
    } else {
      val result = new StringBuffer()
      for (word <- wordList) {
        if (result.length + word.toString.length < 250) {
          result.append(word.toString + "\n")
        }
      }
      result.toString
    }
  }

  def retweetCount(path: String): Int = {
    val retweetDf: DataFrame = spark.read.json(path)

    retweetDf
      .select(flattenSchema(retweetDf.schema):_*)
      .select($"text")
      .as[String]
      .flatMap(_.split("\\s"))
      .map(_.replaceAll("[^\\w#@]+", ""))
      .filter(passesWordFilter _)
      .filter(_.matches("RT"))
      .count()
      .toInt
  }

  /**
   * This method takes the provided text and determines the percentage of the
   * contained Tweets that are retweets.
   *
   * @param path: string containing path to data file.
   * @return      The percentage of tweets in the given data that are retweets.
   */
  def retweetPercentage(path: String): Double = {
    val retweetDf: DataFrame = spark.read.json(path)

    val totalCount: Double = retweetDf
      .select(flattenSchema(retweetDf.schema):_*)
      .select("text")
      .as[String]
      .count()
      .toDouble

    val rtCount = retweetCount(path)

    "%.2f".format((rtCount/totalCount)*100).toDouble
  }

  /**
   * This method takes the provided text and determines the total
   * contained Tweets in it.
   *
   * @param path: string containing path to data file.
   * @return      The total number of tweets in the given data.
   */
  def totalCount(path: String): Int = {
    val retweetDf: DataFrame = spark.read.json(path)

    retweetDf
      .select(flattenSchema(retweetDf.schema):_*)
      .select("text")
      .as[String]
      .count()
      .toInt

  }


  /**
   * This method flattens out all JSON, making our other methods work both on
   * streaming data and Tweet object data.
   * Found at:
   * https://stackoverflow.com/questions/37471346/automatically-and-elegantly-flatten-dataframe-in-spark-sql/38460312
   *
   * @param schema: StructType that describes the current structure of the JSON
   * @param prefix: Customizable prefix to add to columns
   * @return        Flattened JSON object
   */
  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else prefix + "." + f.name

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }

}