package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO operator - mapPartitions
    val rdd = sc.makeRDD(List(1,2,3,4))

    val mpRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        //(0,1),(1,2)
        iter.map(
          num => {
            (index,num)
          }
        )
      }
    )

    mpRDD.collect().foreach(println)

    sc.stop()
  }
}
