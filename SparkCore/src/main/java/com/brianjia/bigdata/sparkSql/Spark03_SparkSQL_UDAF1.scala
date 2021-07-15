package com.brianjia.bigdata.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}


object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    //TODO create SparkSQL running env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //TODO perform logical operation

    val df = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))


    spark.sql("select ageAvg(age) from user").show()

    //TODO close env
    spark.close()
  }

  /**
   * self-defined UDAF class
   * 1. extends org.spark.sql.expressions.Aggregator
   *    1.1 IN: input data type
   *    1.2 BUF: buffer data type
   *    1.3 OUT: output data type
   * 2. rewrite the 6 methods
   */
  case class Buff(
                 var total: Long,
                 var count: Long
                 )

  class MyAvgUDAF extends Aggregator[Long, Buff, Long]{
    override def zero: Buff = {
      Buff(0L,0L)
    }

    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      Buff(b1.total+b2.total, b1.count+b2.count)
    }

    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
