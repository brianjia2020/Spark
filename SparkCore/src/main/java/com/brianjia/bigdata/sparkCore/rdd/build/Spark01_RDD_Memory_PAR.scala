package com.brianjia.bigdata.sparkCore.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_PAR {
  def main(args: Array[String]): Unit = {
    //TODO prepare the env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO create the rdd
    //RDD's parallelization && partition
    //makeRDD method can pass
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4),
    )

    rdd.saveAsTextFile("output")

    //TODO close the env
    sc.stop()
  }
}
