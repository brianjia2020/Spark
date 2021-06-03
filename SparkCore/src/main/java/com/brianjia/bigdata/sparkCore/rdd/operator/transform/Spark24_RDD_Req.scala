package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO - real case analysis

    //1. read in raw file
    val dataRDD = sc.textFile("/Users/chunyangjia/Desktop/jars/studyMaterial/Spark3/material/data/agent.log")
    //2. parse the raw data, split by space
    //   map to => ((province, advertisement), 1)
    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )
    //3. aggregate to sum => ((province, advertisement), sumClick)
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    //4. structural transformation
    //   (province, (ad, sumClick))
    val newMapRDD = reduceRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }
    //5. partition By based on province
    //   (province, [(adA, sumClick),(adB, sumClick)...])
    val groupRDD = newMapRDD.groupByKey()
    //6. orderBy inside each partition and keep only the top three advertisement
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    //7. collect and print
    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
