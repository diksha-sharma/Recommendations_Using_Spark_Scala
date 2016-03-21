

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io._
import java.util._

/*
 * Q4. Using spark/scala
 * Step 1: Calculate the average age of the direct friends of each user.
 * Step 2: Sort the users by the calculated average age from step 1 in descending order.
 * Step 3. Output the top 20 users from step 2 with their address and the calculated average age.
 * Sample output.
 * User A, 1000 Anderson blvd, Dallas, TX, average age of direct friends.
 */

object Q4
{
  def main(args: Array[String]) 
  {
    val conf1 = new SparkConf().setAppName("Q4").setMaster("local")  
    val sc1 = new SparkContext(conf1)
    val friendsFile = sc1.textFile("E:/CS6350 - Big Data Management and Analytics/HW/dataset/soc-LiveJournal1Adj.txt")

    val userDataFile = sc1.textFile("E:/CS6350 - Big Data Management and Analytics/HW/dataset/userdata.txt")
    
    val list1 = friendsFile.map(line1 => line1.split("\t")).flatMap { line3 => line3(1).split(",")}
    //var output = user1 + ", "
    val length = list1.count().toInt
    var avgAge = 0;
    for(i <- 1 to length)
    {
      val friend = list1.take(i).mkString
      val userDetails = userDataFile.map(line1 => line1.split(","))
                  .filter(line2 => line2(0).toString.equals(friend))
                  .map { line3 => (line3(1) , line3(3) , line3(4), line3(5), line3(6), line3(9))}
      
      val year  = userDetails.take(5).toString().split("/").foreach(println)
    }  
  }  
   
  def calculateAge(year: Int): Int = 
  {
    val age = 2016-year
    return age
  }
  
}

















