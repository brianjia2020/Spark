package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - distinct
    // cause shuffling of data
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2,3,4))

    //map(x => (x,null))
    //reduceByKey((x, _) => x)
    //map(_._1)
    val rdd1:RDD[Int] = rdd.distinct()
    
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
