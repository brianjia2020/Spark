package com.brianjia.bigdata.sparkCore.req

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategory")
    val sc = new SparkContext(sparkConf)

    //Issue: actionRDD has been used many times => cache it
    //       cogroup is not efficient

    //1. load raw data
    val actionRDD = sc.textFile("/Users/chunyangjia/Desktop/jars/studyMaterial/Spark3/material/spark-core-data/user_visit_action.txt")
    actionRDD.cache()

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
    val rdd1 = clickCountRDD.map{
      case (cid, cnt) => {
        (cid, (cnt,0,0))
      }
    }

    val rdd2 = orderCountRDD.map{
      case (cid, cnt) => {
        (cid, (0,cnt,0))
      }
    }

    val rdd3 = payCountRDD.map{
      case (cid, cnt) => {
        (cid, (0,0,cnt))
      }
    }
    //link the 3 data sources together and do the aggregation computation
    val sourceRDD = rdd1.union(rdd2).union(rdd3)
    val analysisRDD = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    val resultRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)
    //6. collect and then print out to console
    resultRDD.foreach(println)
    sc.stop()
  }
}
