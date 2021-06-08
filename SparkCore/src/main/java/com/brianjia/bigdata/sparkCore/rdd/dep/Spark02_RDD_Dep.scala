package com.brianjia.bigdata.sparkCore.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark framework
    //TODO build spark frame links
    //JDBC : Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("Brian")
    val sc = new SparkContext(sparkConf)

    //TODO business logic

    //1. read the source, get the line content
    val lines: RDD[String] = sc.makeRDD(List(
      "Hello Spark", "Hello Scala"
    ))
    println(lines.dependencies)
    //2. break each line into individual words
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    val wordToOne = words.map(
      word => (word, 1)
    )
    println(wordToOne.dependencies)
    //3. count the number of words
    val wordToCount = wordToOne.reduceByKey((x,y)=>{x+y})
    println(wordToCount.dependencies)
    println(wordToCount.getNumPartitions)
    //4. print to console
    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples.foreach(println)

    //TODO close the links
    sc.stop()
  }
}
