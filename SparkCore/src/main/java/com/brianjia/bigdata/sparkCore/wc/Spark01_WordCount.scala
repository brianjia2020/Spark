package com.brianjia.bigdata.sparkCore.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark framework
    //TODO build spark frame links
    //JDBC : Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("Brian")
    val sc = new SparkContext(sparkConf)

    //TODO business logic

    //1. read the source, get the line content
    val lines: RDD[String] = sc.textFile("/Users/chunyangjia/IdeaProjects/Spark/data")

    //2. break each line into individual words
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3. count the number of words
    val wordGroup:RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordToCount: RDD[(String, Int)] = wordGroup.map{
      case(word, list) => {
        (word,list.size)
      }
    }

    //4. print to console
    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples.foreach(println)

    //TODO close the links
    sc.stop()
  }
}
