package com.brianjia.bigdata.sparkCore.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello","spark", "hello"))
    //acquire accumulator
    //Spark by default provided simple data aggregation accumulator


    //this way works but has data shuffling
//    rdd.map((_,1)).reduceByKey(_+_)

    //create accumulator class
    //register with spark
    val wcAcc = new MyAccumulator();
    sc.register(wcAcc, "wordCountAcc")
    rdd.foreach(
      word => {
        wcAcc.add(word)
      }
    )

    println(wcAcc.value)
    sc.stop()
  }

  /**
   * self-defined data aggregator: WordCount
   * 1. extends AccumulatorV2, define the data types
   *    IN: input data types
   *    OUT: output data types
   * 2. rewrite the functions
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{
    private var wcMap = mutable.Map[String, Long]()

    //if it is the initial state
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    //merge multiple accumulator
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
