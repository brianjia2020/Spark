package com.brianjia.bigdata.sparkCore.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Broadcast {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a",1), ("b",2), ("c",3)
    ))

//    val rdd2 = sc.makeRDD(List(
//      ("a",4), ("b",5), ("c",6)
//    ))

    //join will cause
//    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    joinRDD.collect().foreach(println)
    val map = mutable.Map(("a",4), ("b",5), ("c",6))
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    rdd1.map{
      case (w, c) => {
        //visit broadcast
        val l = bc.value.getOrElse(w,0L)
        (w,(c,l))
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
