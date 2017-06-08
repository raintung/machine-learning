
package spark.clustering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
   // val data = MLUtils.loadLibSVMFile(sc, "hdfs://192.168.121.101:9000/sparkdata/mllib/kmeans_data.txt")
    val data = sc.textFile("hdfs://192.168.121.101:9000/sparkdata/mllib/TokenMatrix.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
//    val numClusters = 2
//    val numIterations = 20
//    val clusters = KMeans.train(parsedData, numClusters, numIterations)
//
//    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = clusters.computeCost(parsedData)
//    println("Within Set Sum of Squared Errors = " + WSSSE)
    
    for(i <- 0 to 15){
      val model:KMeansModel = KMeans.train(parsedData, i, 30)
      val ssd = model.computeCost(parsedData)
      println("k=" + i + " -> "+ ssd)
    }
//    val ks:Array[Int] = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
//    
//ks.foreach(cluster => {
// val model:KMeansModel = KMeans.train(parsedData, cluster,30)
// val ssd = model.computeCost(parsedData)
// println("k=" + cluster + " -> "+ ssd)
//})
    //clusters.
    // Save and load model
   // clusters.save(sc, "hdfs://192.168.121.101:9000/target/org/apache/spark/KMeansExample/KMeansModel")
    //val sameModel = KMeansModel.load(sc, "hdfs://192.168.121.101:9000/target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
