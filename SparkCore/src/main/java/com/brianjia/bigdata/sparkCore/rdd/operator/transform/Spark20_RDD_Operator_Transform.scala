package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - combineByKey (Key,Value)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),
      ("b",4),("b",5),("a",6)
    ),2)

    /*
        1. reduceByKey: combineByKeyWithClassTag[V](
                          (v: V) => v, //first value mapping operation, the first value will be the same
                          func,  // inside partition operation
                          func,  // between partitions operation
                          partitioner // partitions
                        )
         2. aggregateByKey: combineByKeyWithClassTag  -- same method being called
         3. foldByKey: combineByKeyWithClassTag -- same method being called
         4. combineByKey: combineByKeyWithClassTag -- same method being called
     */

    //all are word count
    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_,_+_)
    rdd.foldByKey(0)(_+_)
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)
    
    sc.stop()
  }
}
