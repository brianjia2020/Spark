package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - groupBy
    // cause shuffling of data
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //groupBy put every data to different group
    //based on returned key value
    def groupFunction(num:Int): Int = {
      num % 1
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)
    sc.stop()
  }
}
