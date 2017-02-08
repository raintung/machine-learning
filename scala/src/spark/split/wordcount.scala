package spark.split

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object wordcount {
   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("wordcount").setMaster("spark://192.168.121.101:7077")
     val sc = new SparkContext(conf)
     sc.addJar("/Users/keli/Documents/machinelearning.jar")
     //val sc = new SparkContext("spark://172.16.114.8:7077","wordcount",System.getenv("SPARK_HOME"),SparkContext.jarOfClass(this.getClass).toSeq, Map())
     val line = sc.textFile("hdfs://192.168.121.101:9000/data/word.txt")
     // val line = sc.textFile(args(0))
     println("start the count1");
     line.flatMap(_.split(" "))
                         .map((_, 2))
                             .reduceByKey(_+_).collect().foreach(println)
     sc.stop()
   }
} 