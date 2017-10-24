

package spark.sort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object sortsample {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("sortsample")
    val sc = new SparkContext(conf)
    var pairs = sc.parallelize(Array(("a",0),("b",0),("c",3),("d",6),("e",0),("f",0),("g",3),("h",6)), 2);
    pairs.sortByKey(true, 4).collect().foreach(println);
  }
}