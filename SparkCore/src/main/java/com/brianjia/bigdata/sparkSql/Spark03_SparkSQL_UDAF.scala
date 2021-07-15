package com.brianjia.bigdata.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    //TODO create SparkSQL running env
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //TODO perform logical operation

    val df = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("ageAvg", new MyAvgUDAF())

    spark.sql("select ageAvg(age) from user").show()

    //TODO close env
    spark.close()
  }

  /**
   * self-defined UDAF class
   * 1. extends UserDefinedAggregateFunction
   * 2. rewrite the 8 methods
   */

  class MyAvgUDAF extends UserDefinedAggregateFunction{
    //input data structure
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    //buffer data structure
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    //output data type
    override def dataType: DataType = LongType

    //
    override def deterministic: Boolean = true

    //buffer initialization
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0)+buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1) + buffer2.getLong(1))
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
