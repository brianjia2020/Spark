package com.brianjia.bigdata.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_Transform {
  def main(args: Array[String]): Unit = {
    //TODO create env
    //StreamingContext: 1. sparkContextConf 2. batch gathering duration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //this two looks very similar, but we need transform
    //1. DStream does not have that function
    //2. need a function to be periodically performed for each batch

    //Code: Driver
    val newDS: DStream[String] = lines.transform(
      rdd => {
        //Code: Driver, (but periodically perform for every batch)
        rdd.map(
          str => {
            //Code: Executor
            str
          }
        )
      }
      )

    //Code : at Driver
    val newDS2: DStream[String] = lines.map(
      data => {
        //Code : at Executor
        data
      }
    )

    //TODO close env
    //spark streaming has to be run continuously and listen to the required port
    //if the main method has been executed and the main program cannot be completed

    //1. start the gatherer
    ssc.start()

    //2. wait for gathering process to finish
    ssc.awaitTermination()
  }

}
