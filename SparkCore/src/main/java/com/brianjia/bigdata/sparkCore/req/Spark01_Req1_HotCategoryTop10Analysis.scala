package com.brianjia.bigdata.sparkCore.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategory")
    val sc = new SparkContext(sparkConf)

    //1. load raw data
    val actionRDD = sc.textFile("/Users/chunyangjia/Desktop/jars/studyMaterial/Spark3/material/spark-core-data/user_visit_action.txt")

    //2. summarize click data by category
    val clickActionRDD = actionRDD.filter (
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)
    //3. summarize order data by category
    val orderActionRDD = actionRDD.filter (
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id,1))
      }
    ).reduceByKey(_+_)

    //4. summarize payment data by category
    val payActionRDD = actionRDD.filter (
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id,1))
      }
    ).reduceByKey(_+_)
    //5. order by category and take top 10
    val cogroupRDD = clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD = cogroupRDD.mapValues{
      case (clickIter, orderIter,payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if(iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if(iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if(iter3.hasNext) {
          payCnt = iter3.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)
    //6. collect and then print out to console
    resultRDD.foreach(println)
    sc.stop()
  }
}
