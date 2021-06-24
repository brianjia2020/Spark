package com.brianjia.bigdata.sparkCore.req

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategory")
    val sc = new SparkContext(sparkConf)

    //Issue: still has shuffle........reduceByKey
    //       use aggregator

    //1. load raw data
    val actionRDD = sc.textFile("/Users/chunyangjia/Desktop/jars/studyMaterial/Spark3/material/spark-core-data/user_visit_action.txt")
    //2. transform the structure to
    val acc = new HotCategoryAccumulator
    sc.register(acc,"hotCategory")
    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          acc.add((datas(6),"click"))
        } else if (datas(8) != "null") {
          val ids = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add((id, "order"))
            }
          )
        } else if (datas(10) != "null") {
          val ids = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add((id, "pay"))
            }
          )
        } else {
          Nil
        }
      }
    )
    //3. aggregation based on cid
    //   (cid, (cnt,cnt,cnt))
    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories = accVal.values

    val analysisList = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )
    //4. order by and collect
    val resultRDD = analysisList.take(10)
    resultRDD.foreach(println)
    sc.stop()
  }
  case class HotCategory(cid:String, var clickCnt:Int,  var orderCnt:Int, var payCnt:Int)
  /**
   * self-defined accumulator
   *    IN: (cid, operationType)
   *    OUT: mutable.Map[String,HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String,String), mutable.Map[String, HotCategory]]{

    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String,String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String,String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category = hcMap.getOrElse(cid, HotCategory(cid,0,0,0))
      if(actionType== "click") {
        category.clickCnt += 1
      } else if (actionType == "order"){
        category.orderCnt += 1
      } else if (actionType == "pay"){
        category.payCnt += 1
      }
      hcMap.update(cid,category)
    }

    override def merge(other: AccumulatorV2[(String,String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value
      map2.foreach{
        case (cid,hc) => {
          val category = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid,category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }
}
