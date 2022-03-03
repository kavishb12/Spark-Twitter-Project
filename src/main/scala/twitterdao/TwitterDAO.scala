package twitterdao

import java.io.{BufferedReader, InputStreamReader, PrintWriter, FileWriter}
import java.nio.file.Paths

import net.liftweb.json._
import org.apache.http.NameValuePair
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.joda.time.DateTime
import twitterobjects.Tweet

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ArrayBuffer




class TwitterDAO(bearerToken: String) {


  def collectStreamedTweets(path:String, count:Int=1000): Unit = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if (null != entity) {
      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
      var line = reader.readLine
      val fileW = new FileWriter(Paths.get(path).toFile, true)
      val fileWriter = new PrintWriter(fileW)
    //   val fileWriter = new PrintWriter(Paths.get(path).toFile)
      var lineNumber = 1 //track line number to know when to move to new file
      while (line != null && lineNumber % count != 0) {
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
      fileWriter.close()
    }
  }

  private def JSONTweetResponseToTweetObject(response: String): List[Tweet] = {

    val jsonResponse = parse(response)
    implicit val formats: DefaultFormats.type = DefaultFormats

    var tweetIdList = ArrayBuffer[Long]()
    var authorIdList = ArrayBuffer[Long]()
    var textList = ArrayBuffer[String]()
    var tweetList = ArrayBuffer[Tweet]()

    val tweetId = (jsonResponse \ "id").children
    tweetId.foreach(tweetIdList += _.extract[Long])

    val authorId = (jsonResponse \ "user" \ "id").children
    authorId.foreach(authorIdList += _.extract[Long])

    val text = (jsonResponse \ "full_text").children
    text.foreach(textList += _.extract[String])


    for (i <- textList.indices)
      tweetList+=Tweet(tweetIdList(i), authorIdList(i), textList(i))

    tweetList.toList

  }

}