package main


import java.io.{BufferedWriter, File, FileWriter, InputStreamReader, PrintWriter}
import java.nio.file.Paths

import spark.analysisEngine
import com.danielasfregola.twitter4s.TwitterRestClient
import net.liftweb.json.{DefaultFormats, parse}
import net.liftweb.json.Serialization.write

import scala.util.{Failure, Success}
import twitterdao.TwitterDAO
import twitterobjects.Tweet

import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

object Main extends App {

val PATH = "C:\\Users\\Kavish Bhathija\\IdeaProjects\\Twitter Spark\\Data\\twitter_data.json"
val PATH_RESULT = "C:\\Users\\Kavish Bhathija\\IdeaProjects\\Twitter Spark\\Data\\twitter_output.txt"
val BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAE1OZwEAAAAATFMB5bablOjzse%2BATYQZJ3oNyOw%3DtciwyMbIJVIDfz68wl3EF3n8RpP2NkIgiZarJTdad5bwxD9Zjb"
val twitterDAO = new TwitterDAO(BEARER_TOKEN) 
val analysisEngine = new analysisEngine()
// twitterDAO.collectStreamedTweets(PATH, 500)
// var i=0
// for(i <- 1 until 6){
//   twitterDAO.collectStreamedTweets(PATH, 800)
//   Thread.sleep(2000) // wait for 1000 millisecond

//   }  

val wordcount = analysisEngine.wordCounter(PATH)
val handles = analysisEngine.handleStripper(PATH)
val hashtags = analysisEngine.hashtagStripper(PATH)
val rtcount = analysisEngine.retweetCount(PATH)
val rtpercentage = analysisEngine.retweetPercentage(PATH)
val totaltweets = analysisEngine.totalCount(PATH)

val fileWriter = new PrintWriter(Paths.get(PATH_RESULT).toFile)
var lineNumber = 1 //track line number to know when to move to new file
fileWriter.println("The count of words is as follows:")
fileWriter.println(wordcount)
fileWriter.println("The Handles are as follows:")
fileWriter.println(handles)
fileWriter.println("The Hashtags are as follows:")
fileWriter.println(hashtags)
fileWriter.println("The total number of tweets is "+totaltweets)
fileWriter.println("The total number of retweets are "+ rtcount+" which is "+rtpercentage+"% of all the tweets.")

fileWriter.close()
  

}