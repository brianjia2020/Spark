package com.brianjia.bigdata.sparkCore.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_word {
  def main(args: Array[String]): Unit = {
    //TODO create the env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO create the rdd
    //1. Data is read line by line
    //    Spark uses Hadoop way to read and read line by line regardless of number of bytes
    //2. Data is read by shift quantity
    /*
      14 bytes / 2 = 7bytes
      14 /7 = 2
     */

    /*
      1234567@@ => 012345678
      89@@ => 9101112
      0 => 13

      [0,7) => 1234567
      [7,14) => 890
     */
    //3. Data partition calculation is based on shift quantity
    //    0 => [0,3)
    //    1 => [3,6)
    //    2 => [6,7)
    val rdd : RDD[String] = sc.textFile("data/word.txt",2)
    rdd.collect().foreach(println)
    rdd.saveAsTextFile("output/")

    //TODO close the env
    sc.stop()
  }
}
