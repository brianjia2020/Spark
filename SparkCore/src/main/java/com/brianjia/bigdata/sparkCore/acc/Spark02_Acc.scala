package com.brianjia.bigdata.sparkCore.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4))
    //acquire accumulator
    //Spark by default provided simple data aggregation accumulator
    val sumAcc = sc.longAccumulator("sum")
    rdd.foreach(
      num => {
        //use accumulator
        sumAcc.add(num)
      }
    )
    println(sumAcc.value)

    sc.stop()
  }
}
