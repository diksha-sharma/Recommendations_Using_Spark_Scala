

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io._
import java.util._

/*
 * Q3.
 * Using spark/scala, Given any two Users as input, output the list of the names and the zipcode of their
 * mutual friends.
 * Note: use the userdata.txt to get the extra user information.
 * Output format:
 * UserA id, UserB id, list of [names:zipcodes] of their mutual Friends.
 */

object Q3
{
  def main(args: Array[String]) 
  {
    
    val user1 = readLine("Enter user 1 id: ").toString()
    val user2 = readLine("Enter user 2 id: ").toString()

    val conf1 = new SparkConf().setAppName("Q3").setMaster("local")  
    val sc1 = new SparkContext(conf1)
    val friendsFile = sc1.textFile("E:/CS6350 - Big Data Management and Analytics/HW/dataset/soc-LiveJournal1Adj.txt")

    val userDataFile = sc1.textFile("E:/CS6350 - Big Data Management and Analytics/HW/dataset/userdata.txt")
    
    val list1 = friendsFile.map(line1 => line1.split("\t"))
                .filter(line2 => line2(0).toString.equals(user1))
                .flatMap { line3 => line3(1).split(",")}
                
    val list2 = friendsFile.map(line1 => line1.split("\t"))
                .filter(line2 => line2(0).toString.equals(user2))
                .flatMap { line3 => line3(1).split(",")}
    
    val mutualFriends = list1.intersection(list2)
    var output = user1 + " " + user2 + " [" 
    val length = mutualFriends.count().toInt
    for(i <- 1 to length)
    {
      val friend = mutualFriends.take(i).mkString
      val userZipCode = userDataFile.map(line1 => line1.split(","))
                  .filter(line2 => line2(0).toString.equals(friend))
                  .map { line3 => (line3(1) , line3(6))}
      val next = userZipCode.collect().deep.mkString(":")
      val nextNew = output + next 
      
      if(i == length)
      {
        output = nextNew
      }
      else
      {
        output = nextNew + ", "
      }
      
    }
    
    output = output + "]"
    print(output)
    
  }  
   
}

















