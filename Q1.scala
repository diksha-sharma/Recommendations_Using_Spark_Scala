import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

/*
 * Q1
 * Write a Scala/Spark program that implements a simple “People You Might
 * Know" social network friendship recommendation algorithm. The key idea is that
 * if two people have a lot of mutual friends, then the system should recommend that they
 * connect with each other.
 */

object Q1{
  
  def main(args: Array[String]) 
  {
    
    val conf = new SparkConf().setAppName("Q1").setMaster("local")
    val sc = new SparkContext(conf)
    val lineRead = sc.textFile("E:/CS6350 - Big Data Management and Analytics/HW/dataset/soc-LiveJournal1Adj.txt").cache();
    val list = lineRead.map(line => splitLine(line));
    val directFriends = getDirectFriends(list)
    val finalList = getNewRecommendations(directFriends)
    val recommendations = finalList.reduceByKey((a,b)=> a+b).map(line1 => (line1._1._1, (line1._1._2, line1._2))).groupByKey().map(line2 => line2._1.toString + "\t" + line2._2.map(x=>"("+x._1 + "," + x._2+")").toArray.mkString(","))
    recommendations.saveAsTextFile("E:/CS6350 - Big Data Management and Analytics/HW/HW2/output/recommendations.txt")
   
  }
  
  def splitLine(line:String ) : (String,Array[String]) = 
  {
      var data = line.split("\t");
      if(data.length == 2)
      {
        var friends = data(1).split(",");
        return (data(0), friends)
      }
      else
      {
        return(data(0), Array[String]());
      }      
  }
  
 	def getNewRecommendations(directFriends : org.apache.spark.rdd.RDD[(Int, Int, Int)]) :  org.apache.spark.rdd.RDD[((Int, Int), Int)] = 
	{
    var list1 = directFriends.map(a => ((a._1,a._2),a._3))
    var list2 = list1.filter(b => b._2 == -1)
    var list3 = list1.map(a => a._1)
    var list4 = list2.map(a => a._1)
    list3 = list3.subtract(list4)
    val list5 = list3.map(a => (a,1))
    return list5
  }
	
	def suggestFriends(people: List[(Int, Int)], n: Int) : List[Int]   = 
	{
    people.sortBy(line => (-line._2, line._1)).take(n).map(line => line._1)
  }
	
	def getDirectFriends(user : org.apache.spark.rdd.RDD[(String, Array[String])]) : org.apache.spark.rdd.RDD[(Int, Int, Int)] =
  {
     var adjacency: List[(Int,Int,Int)] = List();
     var list1 : List[(String,Array[String])] = List();
     var list2 = user.flatMap(f => f._2.map(x =>(x.toInt,f._1.toInt,"-1".toInt)))
     var list3 =(user.flatMap(f => f._2.flatMap { x => f._2.map{x1 => (x.toInt, x1.toInt, f._1.toInt)
               (x1.toInt, x.toInt, f._1.toInt)}}))
     list2.++=(list3)
     var list4 = list2.filter(f => f._1!=f._2)
     return list4     
   }
}