package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - sortBy(cause shuffling)
    // cause shuffling of data
    // coalesce is used to decrease partitions
    // repartition: is used to increase partitions (equal to coalesce and shuffle to be true)
    val rdd = sc.makeRDD(List(("1",1),("11",2),("2",3)),2)
    //true, by fault is ascending
    //false is descending
    rdd.sortBy(t=>t._1.toInt, false).collect().foreach(println)
    sc.stop()
  }
}
