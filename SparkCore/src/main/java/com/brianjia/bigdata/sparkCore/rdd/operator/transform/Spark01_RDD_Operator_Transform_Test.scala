package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO operator - map
    val rdd = sc.textFile("data/word.txt")

    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(0)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
