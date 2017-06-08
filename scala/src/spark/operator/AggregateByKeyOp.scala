package spark.operator

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object AggregateByKeyOp {  
  def main(args:Array[String]){  
     val sparkConf: SparkConf = new SparkConf().setAppName("AggregateByKey").setMaster("local")  
    val sc: SparkContext = new SparkContext(sparkConf)  
       
     val data=List((1,3),(1,2),(1,4),(2,3))  
     val rdd=sc.parallelize(data, 2)  
       
     //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型  
     def combOp(a:Int,b:Int):Int={  
       println("combOp: "+a+"\t"+b)  
       a+b  
     }  
     //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型  
      def seqOp(a:Int,b:Int):Int={  
        println("SeqOp:"+a+"\t"+b)  
        a+b
      }  
      rdd.foreach(println)  
      //zeroValue:中立值,定义返回value的类型，并参与运算  
      //seqOp:用来在同一个partition中合并值  
      //combOp:用来在不同partiton中合并值  
      val aggregateByKeyRDD=rdd.aggregateByKey(0)(seqOp, combOp).collect()
      aggregateByKeyRDD.foreach(println)
      sc.stop()  
  }
}