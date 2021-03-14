package com.brianjia.bigdata.sparkCore.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_02 {
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

    val wordToOne = words.map(
      word => (word, 1)
    )

    //3. count the number of words
//    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(_._1)

//    val wordToCount: RDD[(String, Int)] = wordGroup.map{
//      case(word, list) => {
//        list.reduce(
//          (t1, t2) => {
//            (t1._1, t1._2 + t2._2)
//          }
//        )
//      }
//    }

    val wordToCount = wordToOne.reduceByKey((x,y)=>{x+y})

    //4. print to console
    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples.foreach(println)

    //TODO close the links
    sc.stop()
  }
}
