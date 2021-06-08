package com.brianjia.bigdata.sparkCore.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark framework
    //TODO build spark frame links
    //JDBC : Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("Brian")
    val sc = new SparkContext(sparkConf)

    //TODO business logic


    //TODO close the links
    sc.stop()
  }

  def wordCount1(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val groupRDD = words.groupBy(word => word)
    val wordCount = groupRDD.mapValues(iter => iter.size)
  }

  def wordCount2(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val groupRDD = wordOne.groupByKey()
    val wordCount = groupRDD.mapValues(iter => iter.size)
  }

  def wordCount3(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.reduceByKey(_+_)
  }

  def wordCount4(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.aggregateByKey(0)(_+_,_+_)
  }

  def wordCount5(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.foldByKey(0)(_+_)
  }

  def wordCount6(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.combineByKey(
      v => v,
      (x:Int,y) => x + y,
      (x:Int,y:Int) => x+y
    )
  }

  def wordCount7(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.countByKey()
  }

  def wordCount8(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordCount = words.countByValue()
  }

  def wordCount9(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
//    val wordCount = words.countByValue()
    //[(string, int)...]
    words.map(
      word => {
        mutable.Map[String,Long]((word,1))
      }
    )
  }.reduce(
    (map1,map2) => {
      map2.foreach{
        case (word,count) => {
          val newCount = map1.getOrElse(word,0L) + count
          map1.update(word,newCount)
        }
      }
      map1
    }
  )

}
