

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io._
import java.util._

/*
 * Q2.
 * Using spark/scala Please answer this question by using dataset from Q1.
 * Given any two Users as input, output the list of the user id of their mutual friends.
 * Output format:
 * UserA, UserB list userid of their mutual Friends.
 */

object Q2 
{
  def main(args: Array[String]) 
  {
    
    val user1 = readLine("Enter user 1 id: ").toString()
    val user2 = readLine("Enter user 2 id: ").toString()
    val conf = new SparkConf().setAppName("Q2").setMaster("local")  
    val sc = new SparkContext(conf)
    val friendsFile = sc.textFile("E:/CS6350 - Big Data Management and Analytics/HW/dataset/soc-LiveJournal1Adj.txt")
    
    val list1 = friendsFile.map(line1 => line1.split("\t"))
                .filter(line2 => line2(0).toString.equals(user1))
                .flatMap { line3 => line3(1).split(",")}
                
    val list2 = friendsFile.map(line1 => line1.split("\t"))
                .filter(line2 => line2(0).toString.equals(user2))
                .flatMap { line3 => line3(1).split(",")}
    
    val mutualFriends = list1.intersection(list2)
    print("Users(" + user1 + ", " + user2 +") :\t" + mutualFriends.collect().deep.mkString("\t"))
    
  }   
  
}