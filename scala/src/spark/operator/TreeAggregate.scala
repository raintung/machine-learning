package spark.operator


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext



object TreeAggregate {
  def main(args:Array[String]){ 
    
    val sparkConf: SparkConf = new SparkConf().setAppName("AggregateByKey")  
    val sc: SparkContext = new SparkContext(sparkConf)  
       
  val rdd=sc.makeRDD(1 to 20, 10)  
   //  val rdd=sc.parallelize(data, 5)  
     
     def combOp(a:Int,b:Int):Int={  
       println("combOp: "+a+"\t"+b)  
       a+b  
     }  
     //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型  
      def seqOp(a:Int,b:Int):Int={  
        println("SeqOp:"+a+"\t"+b)  
        a+b
      } 
      
     var v = rdd.treeAggregate(0)(seqOp, combOp, 2)
      println(v)
      
  }
}