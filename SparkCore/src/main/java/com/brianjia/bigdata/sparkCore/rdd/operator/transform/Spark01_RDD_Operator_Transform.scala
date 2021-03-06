package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO operator - map
    val rdd = sc.makeRDD(List(1,2,3,4))

    //transform function
//    def mapFunction(num:Int): Int = {
//      num*2
//    }
//    val mapRDD: RDD[Int]= rdd.map(mapFunction)
    val mapRDD: RDD[Int] = rdd.map(_*2)
    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
