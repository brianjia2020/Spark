package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - combineByKey (Key,Value)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),
      ("b",4),("b",5),("a",6)
    ),2)

    //(a, [1,2]),(a,[3,4])
    //aggregateByKey has three variable list
    //    1.the first one - structural operational on the first set
    //    2.the second one - inside each partition calculation,between each partition
    //    3.the third one -

    val newRDD : RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v,1),
      (t:(Int,Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1:(Int,Int), t2:(Int,Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD: RDD[(String, Int)]= newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }

    resultRDD.collect().foreach(println)

    
    
    sc.stop()
  }
}
