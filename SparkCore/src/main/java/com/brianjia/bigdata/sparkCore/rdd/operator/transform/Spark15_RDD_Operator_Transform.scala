package com.brianjia.bigdata.sparkCore.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO operator - partitionBy(Key,Value)

    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //rdd[int] => PairRDDFunctions[Tuple]
    //partitionBy is to repartition based on the new partition rule
    rdd.map((_,1))
      .partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output")
    sc.stop()
  }
}
